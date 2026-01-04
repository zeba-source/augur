#!/usr/bin/env python3
"""
Fix Contributor Cross-Contamination (Issue #3469)

This script identifies and fixes contributor records where GitLab data was
incorrectly stored in GitHub columns (gh_*) instead of GitLab columns (gl_*).

BACKGROUND:
-----------
Prior to the fix for issue #3469, the extract_needed_gitlab_contributor_data()
function incorrectly populated gh_user_id, gh_login, gh_url, and gh_avatar_url
with GitLab data instead of using the corresponding gl_* columns.

This script:
1. Identifies contaminated records by detecting GitLab URLs in gh_url
2. Moves data from gh_* columns to gl_* columns
3. Sets gh_* columns to NULL for GitLab users
4. Logs all changes for audit trail

USAGE:
------
    # Dry run (recommended first step - shows what would change)
    python scripts/fix_contributor_contamination.py --dry-run

    # Fix with confirmation prompt
    python scripts/fix_contributor_contamination.py

    # Fix without confirmation (use in automation)
    python scripts/fix_contributor_contamination.py --no-confirm

    # Fix specific contributors by ID
    python scripts/fix_contributor_contamination.py --cntrb-ids <id1> <id2> <id3>

    # Use custom database connection
    python scripts/fix_contributor_contamination.py --db-url "postgresql://user:pass@host/db"

⚠️  SAFETY WARNINGS:
--------------------
1. BACKUP YOUR DATABASE before running this script
2. Run with --dry-run first to preview changes
3. Script runs in a transaction and will rollback on error
4. Test on a staging database before production
5. Script creates audit table 'contributor_contamination_fixes' for tracking

REQUIREMENTS:
-------------
    pip install psycopg2-binary sqlalchemy tqdm

Related: https://github.com/chaoss/augur/issues/3469
"""

import sys
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    print("ERROR: SQLAlchemy is required. Install with: pip install sqlalchemy")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("WARNING: tqdm not installed. Progress bars will not be shown.")
    print("Install with: pip install tqdm")
    tqdm = None


# ============================================================================
# Configuration
# ============================================================================

# Patterns to identify GitLab data in GitHub columns
GITLAB_URL_PATTERNS = [
    'gitlab.com',
    'gitlab.org',
    'gitlab.',  # Catches custom GitLab instances
]

# Columns to migrate
MIGRATION_MAPPING = {
    'gh_user_id': 'gl_id',
    'gh_login': 'gl_username',
    'gh_url': 'gl_web_url',
    'gh_avatar_url': 'gl_avatar_url',
}


# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the script."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler for detailed logs
    file_handler = logging.FileHandler(
        f'contributor_fix_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Root logger
    logger = logging.getLogger('contributor_fix')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


# ============================================================================
# Database Connection
# ============================================================================

def get_database_connection(db_url: Optional[str] = None):
    """
    Get database connection using Augur's configuration or provided URL.
    
    Args:
        db_url: Optional database URL. If not provided, uses Augur's config.
    
    Returns:
        Tuple of (engine, session)
    """
    if db_url:
        engine = create_engine(db_url, echo=False)
    else:
        # Try to use Augur's database configuration
        try:
            from augur.application.db.session import DatabaseSession
            session = DatabaseSession()
            return session.connection().engine, session
        except ImportError:
            raise RuntimeError(
                "Could not import Augur's DatabaseSession. "
                "Please provide --db-url or run from Augur installation."
            )
    
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session


# ============================================================================
# Contamination Detection
# ============================================================================

def is_gitlab_url(url: Optional[str]) -> bool:
    """
    Check if a URL is a GitLab URL.
    
    Args:
        url: URL string to check
    
    Returns:
        True if URL contains GitLab patterns
    """
    if not url:
        return False
    
    url_lower = url.lower()
    return any(pattern in url_lower for pattern in GITLAB_URL_PATTERNS)


def identify_contaminated_records(session, cntrb_ids: Optional[List[str]] = None) -> List[Dict]:
    """
    Identify contributor records with GitLab data in GitHub columns.
    
    Detection criteria:
    1. gh_url contains 'gitlab.com' or similar patterns
    2. gh_login is populated but gl_username is NULL
    3. gh_user_id is populated but gl_id is NULL
    
    Args:
        session: Database session
        cntrb_ids: Optional list of specific contributor IDs to check
    
    Returns:
        List of dictionaries containing contaminated record data
    """
    logger = logging.getLogger('contributor_fix')
    
    # Build query
    query = """
        SELECT 
            cntrb_id,
            cntrb_login,
            gh_user_id,
            gh_login,
            gh_url,
            gh_html_url,
            gh_avatar_url,
            gl_id,
            gl_username,
            gl_web_url,
            gl_avatar_url,
            data_collection_date,
            tool_source
        FROM augur_data.contributors
        WHERE 
            -- Has GitHub columns populated
            (gh_url IS NOT NULL OR gh_login IS NOT NULL OR gh_user_id IS NOT NULL)
            -- GitLab columns are empty
            AND (gl_id IS NULL OR gl_username IS NULL)
            -- URL indicates GitLab
            AND (
                gh_url ILIKE '%gitlab.com%'
                OR gh_url ILIKE '%gitlab.org%'
                OR gh_url ILIKE '%gitlab.%'
                OR gh_html_url ILIKE '%gitlab.com%'
                OR gh_html_url ILIKE '%gitlab.org%'
            )
    """
    
    params = {}
    if cntrb_ids:
        query += " AND cntrb_id = ANY(:cntrb_ids)"
        params['cntrb_ids'] = cntrb_ids
    
    query += " ORDER BY data_collection_date DESC"
    
    logger.info("Searching for contaminated contributor records...")
    result = session.execute(text(query), params)
    
    contaminated = []
    for row in result:
        contaminated.append({
            'cntrb_id': str(row.cntrb_id),
            'cntrb_login': row.cntrb_login,
            'gh_user_id': row.gh_user_id,
            'gh_login': row.gh_login,
            'gh_url': row.gh_url,
            'gh_html_url': row.gh_html_url,
            'gh_avatar_url': row.gh_avatar_url,
            'gl_id': row.gl_id,
            'gl_username': row.gl_username,
            'gl_web_url': row.gl_web_url,
            'gl_avatar_url': row.gl_avatar_url,
            'data_collection_date': row.data_collection_date,
            'tool_source': row.tool_source
        })
    
    logger.info(f"Found {len(contaminated)} contaminated records")
    return contaminated


# ============================================================================
# Data Migration
# ============================================================================

def create_audit_table(session):
    """Create table to track contamination fixes."""
    logger = logging.getLogger('contributor_fix')
    
    logger.info("Creating audit table for tracking fixes...")
    
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS augur_data.contributor_contamination_fixes (
            fix_id SERIAL PRIMARY KEY,
            cntrb_id UUID NOT NULL,
            fix_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            old_gh_user_id BIGINT,
            old_gh_login VARCHAR,
            old_gh_url VARCHAR,
            old_gh_avatar_url VARCHAR,
            new_gl_id BIGINT,
            new_gl_username VARCHAR,
            new_gl_web_url VARCHAR,
            new_gl_avatar_url VARCHAR,
            fixed_by VARCHAR DEFAULT 'fix_contributor_contamination.py',
            notes TEXT
        )
    """))
    
    session.commit()
    logger.info("Audit table ready")


def preview_changes(contaminated_records: List[Dict]) -> None:
    """
    Display a preview of changes that would be made.
    
    Args:
        contaminated_records: List of contaminated record dictionaries
    """
    logger = logging.getLogger('contributor_fix')
    
    if not contaminated_records:
        logger.info("✅ No contaminated records found!")
        return
    
    print("\n" + "="*80)
    print("CONTAMINATED RECORDS FOUND")
    print("="*80)
    
    for i, record in enumerate(contaminated_records, 1):
        print(f"\n📋 Record {i}/{len(contaminated_records)}")
        print(f"   cntrb_id: {record['cntrb_id']}")
        print(f"   cntrb_login: {record['cntrb_login']}")
        print(f"\n   ❌ CURRENT (WRONG - GitLab data in GitHub columns):")
        print(f"      gh_user_id: {record['gh_user_id']}")
        print(f"      gh_login: {record['gh_login']}")
        print(f"      gh_url: {record['gh_url']}")
        print(f"      gh_avatar_url: {record['gh_avatar_url']}")
        print(f"\n   ✅ WILL BECOME (CORRECT - GitLab data in GitLab columns):")
        print(f"      gh_user_id: NULL")
        print(f"      gh_login: NULL")
        print(f"      gh_url: NULL")
        print(f"      gh_avatar_url: NULL")
        print(f"      gl_id: {record['gh_user_id']}")
        print(f"      gl_username: {record['gh_login']}")
        print(f"      gl_web_url: {record['gh_url']}")
        print(f"      gl_avatar_url: {record['gh_avatar_url']}")
    
    print("\n" + "="*80)
    print(f"TOTAL RECORDS TO FIX: {len(contaminated_records)}")
    print("="*80 + "\n")


def fix_contaminated_record(session, record: Dict, audit: bool = True) -> bool:
    """
    Fix a single contaminated contributor record.
    
    Args:
        session: Database session
        record: Dictionary containing contaminated record data
        audit: Whether to log the fix in audit table
    
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('contributor_fix')
    
    try:
        cntrb_id = record['cntrb_id']
        
        # Step 1: Log to audit table (if requested)
        if audit:
            session.execute(text("""
                INSERT INTO augur_data.contributor_contamination_fixes (
                    cntrb_id, old_gh_user_id, old_gh_login, old_gh_url, old_gh_avatar_url,
                    new_gl_id, new_gl_username, new_gl_web_url, new_gl_avatar_url,
                    notes
                ) VALUES (
                    :cntrb_id, :old_gh_user_id, :old_gh_login, :old_gh_url, :old_gh_avatar_url,
                    :new_gl_id, :new_gl_username, :new_gl_web_url, :new_gl_avatar_url,
                    :notes
                )
            """), {
                'cntrb_id': cntrb_id,
                'old_gh_user_id': record['gh_user_id'],
                'old_gh_login': record['gh_login'],
                'old_gh_url': record['gh_url'],
                'old_gh_avatar_url': record['gh_avatar_url'],
                'new_gl_id': record['gh_user_id'],
                'new_gl_username': record['gh_login'],
                'new_gl_web_url': record['gh_url'],
                'new_gl_avatar_url': record['gh_avatar_url'],
                'notes': f'Migrated GitLab data from gh_* to gl_* columns (issue #3469)'
            })
        
        # Step 2: Move data from gh_* to gl_* columns and NULL out gh_* columns
        session.execute(text("""
            UPDATE augur_data.contributors
            SET
                -- Move data to GitLab columns
                gl_id = :gh_user_id,
                gl_username = :gh_login,
                gl_web_url = :gh_url,
                gl_avatar_url = :gh_avatar_url,
                
                -- Extract additional GitLab fields from existing data if possible
                gl_state = COALESCE(gl_state, 'active'),
                gl_full_name = COALESCE(gl_full_name, cntrb_full_name),
                
                -- NULL out GitHub columns
                gh_user_id = NULL,
                gh_login = NULL,
                gh_url = NULL,
                gh_html_url = NULL,
                gh_node_id = NULL,
                gh_avatar_url = NULL,
                gh_gravatar_id = NULL,
                gh_followers_url = NULL,
                gh_following_url = NULL,
                gh_gists_url = NULL,
                gh_starred_url = NULL,
                gh_subscriptions_url = NULL,
                gh_organizations_url = NULL,
                gh_repos_url = NULL,
                gh_events_url = NULL,
                gh_received_events_url = NULL,
                gh_type = NULL,
                gh_site_admin = NULL
            WHERE cntrb_id = :cntrb_id
        """), {
            'cntrb_id': cntrb_id,
            'gh_user_id': record['gh_user_id'],
            'gh_login': record['gh_login'],
            'gh_url': record['gh_url'],
            'gh_avatar_url': record['gh_avatar_url']
        })
        
        logger.debug(f"✅ Fixed contributor {cntrb_id} ({record['cntrb_login']})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to fix contributor {record['cntrb_id']}: {e}")
        return False


def fix_all_contaminated_records(
    session,
    contaminated_records: List[Dict],
    dry_run: bool = False
) -> Tuple[int, int]:
    """
    Fix all contaminated contributor records.
    
    Args:
        session: Database session
        contaminated_records: List of contaminated record dictionaries
        dry_run: If True, only preview changes without committing
    
    Returns:
        Tuple of (successful_count, failed_count)
    """
    logger = logging.getLogger('contributor_fix')
    
    if not contaminated_records:
        logger.info("✅ No contaminated records to fix!")
        return 0, 0
    
    if dry_run:
        logger.info("🔍 DRY RUN MODE - No changes will be made")
        preview_changes(contaminated_records)
        return 0, 0
    
    # Create audit table
    create_audit_table(session)
    
    successful = 0
    failed = 0
    
    logger.info(f"Starting to fix {len(contaminated_records)} contaminated records...")
    
    # Use tqdm for progress bar if available
    iterator = tqdm(contaminated_records, desc="Fixing records") if tqdm else contaminated_records
    
    try:
        for record in iterator:
            if fix_contaminated_record(session, record, audit=True):
                successful += 1
            else:
                failed += 1
        
        # Commit the transaction
        logger.info("Committing transaction...")
        session.commit()
        logger.info("✅ Transaction committed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during migration: {e}")
        logger.warning("🔄 Rolling back transaction...")
        session.rollback()
        logger.warning("Transaction rolled back - no changes were made")
        raise
    
    return successful, failed


# ============================================================================
# Verification
# ============================================================================

def verify_fixes(session) -> Dict[str, int]:
    """
    Verify that contamination has been fixed.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary with verification statistics
    """
    logger = logging.getLogger('contributor_fix')
    
    logger.info("Verifying fixes...")
    
    # Check for remaining contaminated records
    remaining = session.execute(text("""
        SELECT COUNT(*) as count
        FROM augur_data.contributors
        WHERE 
            (gh_url ILIKE '%gitlab.com%' OR gh_url ILIKE '%gitlab.org%')
            AND (gl_id IS NULL OR gl_username IS NULL)
    """)).fetchone()
    
    # Count total GitLab contributors (gl_id populated)
    gitlab_total = session.execute(text("""
        SELECT COUNT(*) as count
        FROM augur_data.contributors
        WHERE gl_id IS NOT NULL
    """)).fetchone()
    
    # Count GitHub contributors (gh_user_id populated)
    github_total = session.execute(text("""
        SELECT COUNT(*) as count
        FROM augur_data.contributors
        WHERE gh_user_id IS NOT NULL
    """)).fetchone()
    
    # Count fixes made
    fixes_count = session.execute(text("""
        SELECT COUNT(*) as count
        FROM augur_data.contributor_contamination_fixes
    """)).fetchone()
    
    stats = {
        'remaining_contaminated': remaining.count if remaining else 0,
        'gitlab_contributors': gitlab_total.count if gitlab_total else 0,
        'github_contributors': github_total.count if github_total else 0,
        'total_fixes': fixes_count.count if fixes_count else 0
    }
    
    logger.info(f"Verification results:")
    logger.info(f"  - GitLab contributors: {stats['gitlab_contributors']}")
    logger.info(f"  - GitHub contributors: {stats['github_contributors']}")
    logger.info(f"  - Total fixes made: {stats['total_fixes']}")
    logger.info(f"  - Remaining contaminated: {stats['remaining_contaminated']}")
    
    if stats['remaining_contaminated'] == 0:
        logger.info("✅ No contaminated records remaining!")
    else:
        logger.warning(f"⚠️  {stats['remaining_contaminated']} contaminated records still exist")
    
    return stats


# ============================================================================
# Main Script
# ============================================================================

def main():
    """Main script entry point."""
    parser = argparse.ArgumentParser(
        description='Fix contributor cross-contamination (Issue #3469)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without making changes'
    )
    
    parser.add_argument(
        '--no-confirm',
        action='store_true',
        help='Skip confirmation prompt (use in automation)'
    )
    
    parser.add_argument(
        '--db-url',
        type=str,
        help='Database URL (postgresql://user:pass@host/db). Uses Augur config if not provided.'
    )
    
    parser.add_argument(
        '--cntrb-ids',
        nargs='+',
        help='Fix specific contributor IDs only'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(verbose=args.verbose)
    
    logger.info("="*80)
    logger.info("Contributor Cross-Contamination Fix Script")
    logger.info("Issue: https://github.com/chaoss/augur/issues/3469")
    logger.info("="*80)
    
    # Safety warnings
    if not args.dry_run:
        print("\n⚠️  WARNING: This script will modify your database!")
        print("📋 Recommendations:")
        print("   1. Backup your database before proceeding")
        print("   2. Run with --dry-run first to preview changes")
        print("   3. Test on a staging database first")
        print()
    
    try:
        # Get database connection
        logger.info("Connecting to database...")
        engine, session = get_database_connection(args.db_url)
        logger.info("✅ Connected to database")
        
        # Identify contaminated records
        contaminated = identify_contaminated_records(session, args.cntrb_ids)
        
        if not contaminated:
            logger.info("✅ No contaminated records found! Database is clean.")
            return 0
        
        # Preview changes
        preview_changes(contaminated)
        
        # Confirm before proceeding (unless --no-confirm)
        if not args.dry_run and not args.no_confirm:
            response = input(f"\n❓ Fix {len(contaminated)} contaminated records? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                logger.info("❌ Aborted by user")
                return 1
        
        # Fix contaminated records
        successful, failed = fix_all_contaminated_records(
            session,
            contaminated,
            dry_run=args.dry_run
        )
        
        if args.dry_run:
            logger.info("\n🔍 DRY RUN completed - no changes were made")
            logger.info("Run without --dry-run to apply fixes")
            return 0
        
        # Report results
        logger.info("\n" + "="*80)
        logger.info("MIGRATION COMPLETE")
        logger.info("="*80)
        logger.info(f"✅ Successfully fixed: {successful} records")
        if failed > 0:
            logger.error(f"❌ Failed to fix: {failed} records")
        
        # Verify fixes
        stats = verify_fixes(session)
        
        # Final status
        if stats['remaining_contaminated'] == 0 and failed == 0:
            logger.info("\n🎉 SUCCESS! All contaminated records have been fixed.")
            return 0
        else:
            logger.warning("\n⚠️  Some issues remain. Check logs for details.")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Error: {e}", exc_info=True)
        return 1
    finally:
        if 'session' in locals():
            session.close()
            logger.info("Database connection closed")


if __name__ == '__main__':
    sys.exit(main())
