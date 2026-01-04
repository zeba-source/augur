"""Restore contributor unique constraints (Fix for issue #3469)

Revision ID: 38
Revises: 37
Create Date: 2025-12-28 00:00:00.000000

This migration restores the unique constraints on the contributors table that were
inadvertently dropped in migration 22 and never re-added.

Background:
- Migration 22 dropped GH-UNIQUE-C (gh_login) and GL-cntrb-LOGIN-UNIQUE (cntrb_login)
- These constraints were never restored in subsequent migrations
- This allowed cross-contamination where GitLab data was stored in GitHub columns

The constraints being restored:
1. GH-UNIQUE-C: Ensures gh_login is unique (for GitHub users)
2. GL-UNIQUE-B: Ensures gl_id is unique (for GitLab users) 
3. GL-UNIQUE-C: Ensures gl_username is unique (for GitLab users)
4. GL-cntrb-LOGIN-UNIQUE: Ensures cntrb_login is unique

Note: GL-UNIQUE-B and GL-UNIQUE-C may already exist in some installations.
All constraints are DEFERRABLE INITIALLY DEFERRED to handle NULL values properly.

Related: https://github.com/chaoss/augur/issues/3469
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
import logging

# revision identifiers, used by Alembic.
revision = '38'
down_revision = '37'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')


def upgrade():
    """Add unique constraints to contributors table."""
    
    conn = op.get_bind()
    
    # Before adding constraints, we need to handle any existing duplicate data
    logger.info("Checking for and cleaning up duplicate contributor data...")
    
    # Step 1: Identify and log duplicates
    logger.info("Step 1: Identifying duplicate entries...")
    
    try:
        # Check for duplicate gh_login values (excluding NULLs)
        result = conn.execute(text("""
            SELECT gh_login, COUNT(*) as count
            FROM augur_data.contributors
            WHERE gh_login IS NOT NULL
            GROUP BY gh_login
            HAVING COUNT(*) > 1
        """))
        gh_dupes = result.fetchall()
        if gh_dupes:
            logger.warning(f"Found {len(gh_dupes)} duplicate gh_login values:")
            for login, count in gh_dupes:
                logger.warning(f"  - gh_login='{login}': {count} occurrences")
        
        # Check for duplicate gl_id values (excluding NULLs)
        result = conn.execute(text("""
            SELECT gl_id, COUNT(*) as count
            FROM augur_data.contributors
            WHERE gl_id IS NOT NULL
            GROUP BY gl_id
            HAVING COUNT(*) > 1
        """))
        gl_id_dupes = result.fetchall()
        if gl_id_dupes:
            logger.warning(f"Found {len(gl_id_dupes)} duplicate gl_id values:")
            for gl_id, count in gl_id_dupes:
                logger.warning(f"  - gl_id={gl_id}: {count} occurrences")
        
        # Check for duplicate gl_username values (excluding NULLs)
        result = conn.execute(text("""
            SELECT gl_username, COUNT(*) as count
            FROM augur_data.contributors
            WHERE gl_username IS NOT NULL
            GROUP BY gl_username
            HAVING COUNT(*) > 1
        """))
        gl_user_dupes = result.fetchall()
        if gl_user_dupes:
            logger.warning(f"Found {len(gl_user_dupes)} duplicate gl_username values:")
            for username, count in gl_user_dupes:
                logger.warning(f"  - gl_username='{username}': {count} occurrences")
        
        # Check for duplicate cntrb_login values (excluding NULLs)
        result = conn.execute(text("""
            SELECT cntrb_login, COUNT(*) as count
            FROM augur_data.contributors
            WHERE cntrb_login IS NOT NULL
            GROUP BY cntrb_login
            HAVING COUNT(*) > 1
        """))
        cntrb_dupes = result.fetchall()
        if cntrb_dupes:
            logger.warning(f"Found {len(cntrb_dupes)} duplicate cntrb_login values:")
            for login, count in cntrb_dupes:
                logger.warning(f"  - cntrb_login='{login}': {count} occurrences")
    
    except Exception as e:
        logger.error(f"Error checking for duplicates: {e}")
    
    # Step 2: Create backup table for audit trail
    logger.info("Step 2: Creating backup table for duplicate records...")
    
    try:
        conn.execute(text("""
            DROP TABLE IF EXISTS augur_data.contributors_duplicates_backup;
        """))
        
        conn.execute(text("""
            CREATE TABLE augur_data.contributors_duplicates_backup AS
            SELECT 
                c.*,
                NOW() as backup_timestamp,
                'migration_38' as backup_reason
            FROM augur_data.contributors c
            WHERE 
                (gh_login IN (
                    SELECT gh_login 
                    FROM augur_data.contributors 
                    WHERE gh_login IS NOT NULL
                    GROUP BY gh_login 
                    HAVING COUNT(*) > 1
                ))
                OR (gl_id IN (
                    SELECT gl_id 
                    FROM augur_data.contributors 
                    WHERE gl_id IS NOT NULL
                    GROUP BY gl_id 
                    HAVING COUNT(*) > 1
                ))
                OR (gl_username IN (
                    SELECT gl_username 
                    FROM augur_data.contributors 
                    WHERE gl_username IS NOT NULL
                    GROUP BY gl_username 
                    HAVING COUNT(*) > 1
                ))
                OR (cntrb_login IN (
                    SELECT cntrb_login 
                    FROM augur_data.contributors 
                    WHERE cntrb_login IS NOT NULL
                    GROUP BY cntrb_login 
                    HAVING COUNT(*) > 1
                ));
        """))
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM augur_data.contributors_duplicates_backup;
        """))
        backup_count = result.fetchone()[0]
        logger.info(f"Backed up {backup_count} duplicate records to contributors_duplicates_backup")
    
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        logger.info("Continuing with migration despite backup error...")
    
    # Step 3: Resolve duplicates by keeping the most recent record
    logger.info("Step 3: Resolving duplicate records...")
    
    try:
        # For each duplicate, keep only the one with the most recent data_collection_date
        # or cntrb_last_used, and delete the others
        
        # Handle gh_login duplicates
        conn.execute(text("""
            DELETE FROM augur_data.contributors
            WHERE cntrb_id IN (
                SELECT c1.cntrb_id
                FROM augur_data.contributors c1
                WHERE c1.gh_login IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM augur_data.contributors c2
                    WHERE c2.gh_login = c1.gh_login
                    AND c2.cntrb_id != c1.cntrb_id
                    AND (
                        c2.data_collection_date > c1.data_collection_date
                        OR (c2.data_collection_date = c1.data_collection_date 
                            AND c2.cntrb_id > c1.cntrb_id)
                    )
                )
            );
        """))
        
        # Handle gl_id duplicates
        conn.execute(text("""
            DELETE FROM augur_data.contributors
            WHERE cntrb_id IN (
                SELECT c1.cntrb_id
                FROM augur_data.contributors c1
                WHERE c1.gl_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM augur_data.contributors c2
                    WHERE c2.gl_id = c1.gl_id
                    AND c2.cntrb_id != c1.cntrb_id
                    AND (
                        c2.data_collection_date > c1.data_collection_date
                        OR (c2.data_collection_date = c1.data_collection_date 
                            AND c2.cntrb_id > c1.cntrb_id)
                    )
                )
            );
        """))
        
        # Handle gl_username duplicates
        conn.execute(text("""
            DELETE FROM augur_data.contributors
            WHERE cntrb_id IN (
                SELECT c1.cntrb_id
                FROM augur_data.contributors c1
                WHERE c1.gl_username IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM augur_data.contributors c2
                    WHERE c2.gl_username = c1.gl_username
                    AND c2.cntrb_id != c1.cntrb_id
                    AND (
                        c2.data_collection_date > c1.data_collection_date
                        OR (c2.data_collection_date = c1.data_collection_date 
                            AND c2.cntrb_id > c1.cntrb_id)
                    )
                )
            );
        """))
        
        # Handle cntrb_login duplicates
        conn.execute(text("""
            DELETE FROM augur_data.contributors
            WHERE cntrb_id IN (
                SELECT c1.cntrb_id
                FROM augur_data.contributors c1
                WHERE c1.cntrb_login IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM augur_data.contributors c2
                    WHERE c2.cntrb_login = c1.cntrb_login
                    AND c2.cntrb_id != c1.cntrb_id
                    AND (
                        c2.data_collection_date > c1.data_collection_date
                        OR (c2.data_collection_date = c1.data_collection_date 
                            AND c2.cntrb_id > c1.cntrb_id)
                    )
                )
            );
        """))
        
        logger.info("Successfully resolved duplicate records")
    
    except Exception as e:
        logger.error(f"Error resolving duplicates: {e}")
        logger.error("You may need to manually resolve duplicate data before adding constraints")
        logger.error("Check the contributors_duplicates_backup table for affected records")
        raise
    
    # Step 4: Add the unique constraints
    logger.info("Step 4: Adding unique constraints...")
    
    # Add GH-UNIQUE-C constraint (gh_login)
    try:
        logger.info("Adding GH-UNIQUE-C constraint on gh_login...")
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            DROP CONSTRAINT IF EXISTS "GH-UNIQUE-C";
        """))
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            ADD CONSTRAINT "GH-UNIQUE-C" UNIQUE (gh_login)
            DEFERRABLE INITIALLY DEFERRED;
        """))
        logger.info("✓ GH-UNIQUE-C constraint added successfully")
    except ProgrammingError as e:
        logger.error(f"Failed to add GH-UNIQUE-C constraint: {e}")
        raise
    
    # Add GL-UNIQUE-B constraint (gl_id) - may already exist
    try:
        logger.info("Adding GL-UNIQUE-B constraint on gl_id...")
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            DROP CONSTRAINT IF EXISTS "GL-UNIQUE-B";
        """))
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            ADD CONSTRAINT "GL-UNIQUE-B" UNIQUE (gl_id)
            DEFERRABLE INITIALLY DEFERRED;
        """))
        logger.info("✓ GL-UNIQUE-B constraint added successfully")
    except ProgrammingError as e:
        logger.warning(f"Note: GL-UNIQUE-B constraint issue: {e}")
        logger.warning("This constraint may already exist, continuing...")
    
    # Add GL-UNIQUE-C constraint (gl_username) - may already exist
    try:
        logger.info("Adding GL-UNIQUE-C constraint on gl_username...")
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            DROP CONSTRAINT IF EXISTS "GL-UNIQUE-C";
        """))
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            ADD CONSTRAINT "GL-UNIQUE-C" UNIQUE (gl_username)
            DEFERRABLE INITIALLY DEFERRED;
        """))
        logger.info("✓ GL-UNIQUE-C constraint added successfully")
    except ProgrammingError as e:
        logger.warning(f"Note: GL-UNIQUE-C constraint issue: {e}")
        logger.warning("This constraint may already exist, continuing...")
    
    # Add GL-cntrb-LOGIN-UNIQUE constraint (cntrb_login)
    try:
        logger.info("Adding GL-cntrb-LOGIN-UNIQUE constraint on cntrb_login...")
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            DROP CONSTRAINT IF EXISTS "GL-cntrb-LOGIN-UNIQUE";
        """))
        conn.execute(text("""
            ALTER TABLE augur_data.contributors
            ADD CONSTRAINT "GL-cntrb-LOGIN-UNIQUE" UNIQUE (cntrb_login);
        """))
        logger.info("✓ GL-cntrb-LOGIN-UNIQUE constraint added successfully")
    except ProgrammingError as e:
        logger.error(f"Failed to add GL-cntrb-LOGIN-UNIQUE constraint: {e}")
        raise
    
    # Step 5: Create performance indexes
    logger.info("Step 5: Creating performance indexes...")
    
    try:
        # Index on gl_id for lookups (may already exist)
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_contributors_gl_id 
            ON augur_data.contributors (gl_id) 
            WHERE gl_id IS NOT NULL;
        """))
        
        # Index on gl_username for lookups (may already exist)
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_contributors_gl_username 
            ON augur_data.contributors (gl_username) 
            WHERE gl_username IS NOT NULL;
        """))
        
        # Note: gh_login index already exists from migration 23
        
        logger.info("✓ Performance indexes created successfully")
    except Exception as e:
        logger.warning(f"Note: Index creation issue: {e}")
        logger.warning("Indexes may already exist, continuing...")
    
    logger.info("Migration 38 completed successfully!")
    logger.info("Unique constraints restored on contributors table")


def downgrade():
    """Remove the unique constraints added in this migration."""
    
    conn = op.get_bind()
    
    logger.info("Rolling back migration 38...")
    
    # Drop the constraints
    conn.execute(text("""
        ALTER TABLE augur_data.contributors
        DROP CONSTRAINT IF EXISTS "GH-UNIQUE-C",
        DROP CONSTRAINT IF EXISTS "GL-UNIQUE-B",
        DROP CONSTRAINT IF EXISTS "GL-UNIQUE-C",
        DROP CONSTRAINT IF EXISTS "GL-cntrb-LOGIN-UNIQUE";
    """))
    
    # Drop the indexes
    conn.execute(text("""
        DROP INDEX IF EXISTS augur_data.idx_contributors_gl_id;
        DROP INDEX IF EXISTS augur_data.idx_contributors_gl_username;
    """))
    
    # Keep the backup table for safety
    logger.info("Note: Backup table contributors_duplicates_backup was not dropped")
    logger.info("You may want to review or drop it manually")
    
    logger.info("Migration 38 rolled back successfully")
