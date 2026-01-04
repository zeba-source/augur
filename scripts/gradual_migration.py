#!/usr/bin/env python3
"""
Gradual Migration Script for PR #3469
======================================

Migrates contaminated contributor records in small batches to minimize
database lock contention and avoid long maintenance windows.

This script is designed for large production databases where immediate
migration would take too long or cause performance issues.

Usage:
    python scripts/gradual_migration.py [OPTIONS]

Options:
    --batch-size INT     Number of records per batch (default: 1000)
    --sleep FLOAT        Seconds to wait between batches (default: 1.0)
    --db-url STRING      Database connection URL (default: from env)
    --dry-run            Preview without making changes
    --verbose            Show detailed progress

Examples:
    # Conservative: Small batches, longer sleep
    python scripts/gradual_migration.py --batch-size=500 --sleep=2

    # Aggressive: Large batches, minimal sleep
    python scripts/gradual_migration.py --batch-size=5000 --sleep=0.5

    # Preview only
    python scripts/gradual_migration.py --dry-run --verbose
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime
from typing import Optional
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GradualMigration:
    """Handles gradual migration of contaminated contributor records."""
    
    def __init__(
        self, 
        db_url: str,
        batch_size: int = 1000,
        sleep_seconds: float = 1.0,
        dry_run: bool = False,
        verbose: bool = False
    ):
        """
        Initialize gradual migration.
        
        Args:
            db_url: Database connection URL
            batch_size: Number of records per batch
            sleep_seconds: Seconds to wait between batches
            dry_run: If True, preview without making changes
            verbose: If True, show detailed progress
        """
        self.db_url = db_url
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.dry_run = dry_run
        self.verbose = verbose
        
        self.engine: Optional[Engine] = None
        self.total_contaminated = 0
        self.total_migrated = 0
        self.start_time = None
        
    def connect(self):
        """Connect to database."""
        logger.info(f"Connecting to database...")
        self.engine = create_engine(self.db_url)
        
        # Test connection
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.scalar()
            logger.info(f"Connected to PostgreSQL: {version}")
    
    def count_contaminated_records(self) -> int:
        """Count contaminated contributor records."""
        query = """
            SELECT COUNT(*) FROM augur_data.contributors
            WHERE 
                -- Has GitHub columns populated
                (gh_url IS NOT NULL OR gh_login IS NOT NULL OR gh_user_id IS NOT NULL)
                -- GitLab columns are empty
                AND (gl_id IS NULL AND gl_username IS NULL)
                -- URL indicates GitLab
                AND (
                    gh_url ILIKE '%gitlab.com%'
                    OR gh_url ILIKE '%gitlab.org%'
                    OR gh_url ILIKE '%gitlab.%'
                );
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            count = result.scalar()
            return count
    
    def preview_contaminated_records(self, limit: int = 10):
        """Preview contaminated records."""
        query = """
            SELECT 
                cntrb_id,
                cntrb_login,
                gh_user_id,
                gh_login,
                gh_url,
                gl_id,
                gl_username,
                gl_web_url
            FROM augur_data.contributors
            WHERE 
                (gh_url IS NOT NULL OR gh_login IS NOT NULL OR gh_user_id IS NOT NULL)
                AND (gl_id IS NULL AND gl_username IS NULL)
                AND (
                    gh_url ILIKE '%gitlab.com%'
                    OR gh_url ILIKE '%gitlab.org%'
                    OR gh_url ILIKE '%gitlab.%'
                )
            LIMIT :limit;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            records = result.fetchall()
            
            if records:
                logger.info(f"\nPreview of {len(records)} contaminated records:")
                for record in records:
                    logger.info(
                        f"  - {record.cntrb_login}: "
                        f"gh_user_id={record.gh_user_id} → gl_id, "
                        f"gh_url={record.gh_url[:50]}... → gl_web_url"
                    )
    
    def migrate_batch(self) -> int:
        """
        Migrate one batch of contaminated records.
        
        Returns:
            Number of records migrated in this batch
        """
        if self.dry_run:
            # In dry-run mode, just count what would be migrated
            query = """
                SELECT COUNT(*) FROM augur_data.contributors
                WHERE 
                    (gh_url IS NOT NULL OR gh_login IS NOT NULL OR gh_user_id IS NOT NULL)
                    AND (gl_id IS NULL AND gl_username IS NULL)
                    AND (
                        gh_url ILIKE '%gitlab.com%'
                        OR gh_url ILIKE '%gitlab.org%'
                        OR gh_url ILIKE '%gitlab.%'
                    )
                LIMIT :batch_size;
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"batch_size": self.batch_size})
                return min(result.scalar(), self.batch_size)
        
        # Real migration
        query = """
            WITH contaminated_batch AS (
                SELECT cntrb_id 
                FROM augur_data.contributors
                WHERE 
                    (gh_url IS NOT NULL OR gh_login IS NOT NULL OR gh_user_id IS NOT NULL)
                    AND (gl_id IS NULL AND gl_username IS NULL)
                    AND (
                        gh_url ILIKE '%gitlab.com%'
                        OR gh_url ILIKE '%gitlab.org%'
                        OR gh_url ILIKE '%gitlab.%'
                    )
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED  -- Prevent lock contention
            ),
            backup_records AS (
                -- Backup to audit table
                INSERT INTO augur_data.contributor_contamination_fixes (
                    cntrb_id,
                    fix_timestamp,
                    old_gh_user_id,
                    old_gh_login,
                    old_gh_url,
                    old_gh_avatar_url,
                    new_gl_id,
                    new_gl_username,
                    new_gl_web_url,
                    new_gl_avatar_url,
                    fix_method
                )
                SELECT 
                    c.cntrb_id,
                    CURRENT_TIMESTAMP,
                    c.gh_user_id,
                    c.gh_login,
                    c.gh_url,
                    c.gh_avatar_url,
                    c.gh_user_id,  -- Will become gl_id
                    c.gh_login,    -- Will become gl_username
                    c.gh_url,      -- Will become gl_web_url
                    c.gh_avatar_url,  -- Will become gl_avatar_url
                    'gradual_migration'
                FROM augur_data.contributors c
                JOIN contaminated_batch cb ON c.cntrb_id = cb.cntrb_id
                RETURNING cntrb_id
            ),
            migrated_data AS (
                UPDATE augur_data.contributors c
                SET 
                    -- Move GitHub data to GitLab columns
                    gl_id = c.gh_user_id,
                    gl_username = c.gh_login,
                    gl_web_url = c.gh_url,
                    gl_avatar_url = c.gh_avatar_url,
                    gl_state = 'active',  -- Default state
                    
                    -- Clear all GitHub columns
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
                FROM contaminated_batch cb
                WHERE c.cntrb_id = cb.cntrb_id
                RETURNING c.cntrb_id
            )
            SELECT COUNT(*) FROM migrated_data;
        """
        
        with self.engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                result = conn.execute(text(query), {"batch_size": self.batch_size})
                count = result.scalar()
                
                # Commit transaction
                trans.commit()
                
                return count
                
            except Exception as e:
                # Rollback on error
                trans.rollback()
                logger.error(f"Batch migration failed: {e}")
                raise
    
    def run(self):
        """Run gradual migration."""
        self.start_time = datetime.now()
        
        # Connect
        self.connect()
        
        # Count contaminated records
        logger.info("Counting contaminated records...")
        self.total_contaminated = self.count_contaminated_records()
        
        if self.total_contaminated == 0:
            logger.info("✓ No contaminated records found. Database is clean!")
            return
        
        logger.info(f"Found {self.total_contaminated:,} contaminated records")
        
        if self.dry_run:
            logger.info("[DRY RUN] No changes will be made")
        
        # Preview some records
        if self.verbose:
            self.preview_contaminated_records(limit=5)
        
        # Estimate time
        batches = (self.total_contaminated + self.batch_size - 1) // self.batch_size
        estimated_time = batches * (self.sleep_seconds + 0.5)  # 0.5s per batch for query
        logger.info(
            f"\nMigration plan:"
            f"\n  - Total records: {self.total_contaminated:,}"
            f"\n  - Batch size: {self.batch_size:,}"
            f"\n  - Batches: {batches:,}"
            f"\n  - Sleep between batches: {self.sleep_seconds}s"
            f"\n  - Estimated time: {estimated_time:.0f} seconds (~{estimated_time/60:.1f} minutes)"
        )
        
        if not self.dry_run:
            response = input("\nProceed with migration? [y/N]: ")
            if response.lower() != 'y':
                logger.info("Migration cancelled")
                return
        
        # Migrate in batches
        logger.info("\nStarting migration...")
        
        remaining = self.total_contaminated
        
        with tqdm(total=self.total_contaminated, desc="Migrating") as pbar:
            while remaining > 0:
                try:
                    # Migrate one batch
                    migrated = self.migrate_batch()
                    
                    if migrated == 0:
                        break  # No more records to migrate
                    
                    self.total_migrated += migrated
                    remaining -= migrated
                    pbar.update(migrated)
                    
                    if self.verbose:
                        logger.info(
                            f"Batch complete: {migrated} records migrated, "
                            f"{remaining:,} remaining"
                        )
                    
                    # Sleep between batches to reduce load
                    if remaining > 0 and not self.dry_run:
                        time.sleep(self.sleep_seconds)
                
                except KeyboardInterrupt:
                    logger.warning("\n⚠ Migration interrupted by user")
                    logger.info(f"Progress: {self.total_migrated:,}/{self.total_contaminated:,} records migrated")
                    sys.exit(1)
                
                except Exception as e:
                    logger.error(f"Migration error: {e}")
                    logger.info(f"Progress: {self.total_migrated:,}/{self.total_contaminated:,} records migrated")
                    raise
        
        # Final verification
        remaining_contaminated = self.count_contaminated_records()
        
        # Summary
        duration = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(
            f"\n{'DRY RUN ' if self.dry_run else ''}Migration complete!"
            f"\n  - Records migrated: {self.total_migrated:,}"
            f"\n  - Remaining contaminated: {remaining_contaminated:,}"
            f"\n  - Duration: {duration:.1f} seconds (~{duration/60:.1f} minutes)"
            f"\n  - Throughput: {self.total_migrated/duration:.0f} records/second"
        )
        
        if remaining_contaminated > 0:
            logger.warning(
                f"\n⚠ Warning: {remaining_contaminated:,} contaminated records remain"
                f"\n  Run again to continue migration"
            )
        else:
            logger.info("\n✓ All contaminated records migrated successfully!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Gradual migration of contaminated contributor records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records per batch (default: 1000)'
    )
    
    parser.add_argument(
        '--sleep',
        type=float,
        default=1.0,
        help='Seconds to wait between batches (default: 1.0)'
    )
    
    parser.add_argument(
        '--db-url',
        type=str,
        default=None,
        help='Database connection URL (default: from AUGUR_DB_URL env var)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview without making changes'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed progress'
    )
    
    args = parser.parse_args()
    
    # Get database URL
    db_url = args.db_url or os.getenv('AUGUR_DB_URL')
    
    if not db_url:
        logger.error(
            "Database URL not provided. "
            "Set AUGUR_DB_URL environment variable or use --db-url"
        )
        sys.exit(1)
    
    # Run migration
    try:
        migration = GradualMigration(
            db_url=db_url,
            batch_size=args.batch_size,
            sleep_seconds=args.sleep,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        
        migration.run()
        
    except KeyboardInterrupt:
        logger.warning("\n⚠ Migration interrupted")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
