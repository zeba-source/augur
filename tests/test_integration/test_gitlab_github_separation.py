"""
Integration tests for GitHub and GitLab contributor separation (Issue #3469)

This test suite performs end-to-end integration testing with the actual database
to verify that:
1. GitHub contributors use gh_* columns exclusively
2. GitLab contributors use gl_* columns exclusively
3. No cross-contamination occurs between forges
4. Uniqueness constraints work correctly
5. Same username can exist for both GitHub and GitLab (different users)
6. Data retrieval works correctly for each forge

Related: https://github.com/chaoss/augur/issues/3469
"""

import pytest
import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Import Augur models and functions
from augur.application.db.models import Contributor
from augur.application.db.session import DatabaseSession
from augur.tasks.util.contributor_utils import validate_contributor_data
from augur.application.db.data_parse import (
    extract_needed_contributor_data,
    extract_needed_gitlab_contributor_data
)


# ============================================================================
# Pytest Fixtures
# ============================================================================

@pytest.fixture(scope='function')
def database_session():
    """
    Provide a database session for testing with automatic rollback.
    Uses the Augur database configuration.
    """
    # Get database session from Augur's DatabaseSession
    session = DatabaseSession()
    
    yield session
    
    # Cleanup: rollback any changes made during the test
    session.rollback()
    session.close()


@pytest.fixture(scope='function')
def cleanup_test_contributors(database_session):
    """
    Fixture to ensure test contributors are cleaned up after each test.
    Removes any contributors with test-specific usernames.
    """
    test_usernames = [
        'test_github_user_12345',
        'test_gitlab_user_12345',
        'shared_username_test',
        'octocat_test',
        'computationalmystic_test'
    ]
    
    yield
    
    # Cleanup after test
    try:
        for username in test_usernames:
            # Delete GitHub users with this login
            database_session.execute(
                text("DELETE FROM augur_data.contributors WHERE gh_login = :username"),
                {'username': username}
            )
            # Delete GitLab users with this username
            database_session.execute(
                text("DELETE FROM augur_data.contributors WHERE gl_username = :username"),
                {'username': username}
            )
            # Delete by cntrb_login
            database_session.execute(
                text("DELETE FROM augur_data.contributors WHERE cntrb_login LIKE :pattern"),
                {'pattern': f'%test%'}
            )
        
        database_session.commit()
    except Exception as e:
        print(f"Cleanup warning: {e}")
        database_session.rollback()


@pytest.fixture
def sample_github_contributor_data():
    """Sample GitHub contributor data matching API response structure."""
    return {
        'login': 'test_github_user_12345',
        'id': 999999,
        'node_id': 'MDQ6VXNlcjk5OTk5OQ==',
        'avatar_url': 'https://avatars.githubusercontent.com/u/999999',
        'url': 'https://api.github.com/users/test_github_user_12345',
        'html_url': 'https://github.com/test_github_user_12345',
        'type': 'User',
        'site_admin': False,
        'name': 'Test GitHub User',
        'company': 'Test Company',
        'location': 'Test City',
        'email': 'github@test.com'
    }


@pytest.fixture
def sample_gitlab_contributor_data():
    """Sample GitLab contributor data matching API response structure."""
    return {
        'id': 888888,
        'username': 'test_gitlab_user_12345',
        'name': 'Test GitLab User',
        'state': 'active',
        'avatar_url': 'https://secure.gravatar.com/avatar/test',
        'web_url': 'https://gitlab.com/test_gitlab_user_12345'
    }


@pytest.fixture
def sample_shared_username_github():
    """GitHub user with a username that will be shared with GitLab user."""
    return {
        'login': 'shared_username_test',
        'id': 777777,
        'url': 'https://api.github.com/users/shared_username_test',
        'avatar_url': 'https://avatars.githubusercontent.com/u/777777',
        'html_url': 'https://github.com/shared_username_test'
    }


@pytest.fixture
def sample_shared_username_gitlab():
    """GitLab user with a username that will be shared with GitHub user."""
    return {
        'id': 666666,
        'username': 'shared_username_test',
        'web_url': 'https://gitlab.com/shared_username_test',
        'avatar_url': 'https://gitlab.com/avatar.png',
        'name': 'Shared Username Test',
        'state': 'active'
    }


# ============================================================================
# Integration Test: Complete Workflow
# ============================================================================

class TestGitLabGitHubSeparation:
    """
    Integration tests for complete GitHub/GitLab contributor separation workflow.
    """
    
    def test_insert_github_contributor(
        self,
        database_session,
        cleanup_test_contributors,
        sample_github_contributor_data
    ):
        """Test inserting a GitHub contributor with gh_* columns."""
        # Extract contributor data using the fixed function
        contributor_data = extract_needed_contributor_data(sample_github_contributor_data)
        
        # Validate the data
        validate_contributor_data(contributor_data, 'github')
        
        # Insert into database
        cntrb_id = str(uuid.uuid4())
        contributor_data['cntrb_id'] = cntrb_id
        
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login,
                    gh_user_id, gh_login, gh_url, gh_avatar_url, gh_node_id, gh_type,
                    gl_id, gl_username, gl_web_url
                ) VALUES (
                    :cntrb_id, :cntrb_login,
                    :gh_user_id, :gh_login, :gh_url, :gh_avatar_url, :gh_node_id, :gh_type,
                    :gl_id, :gl_username, :gl_web_url
                )
            """),
            {
                'cntrb_id': cntrb_id,
                'cntrb_login': contributor_data['cntrb_login'],
                'gh_user_id': contributor_data['gh_user_id'],
                'gh_login': contributor_data['gh_login'],
                'gh_url': contributor_data['gh_url'],
                'gh_avatar_url': contributor_data['gh_avatar_url'],
                'gh_node_id': contributor_data['gh_node_id'],
                'gh_type': contributor_data['gh_type'],
                'gl_id': contributor_data['gl_id'],
                'gl_username': contributor_data['gl_username'],
                'gl_web_url': contributor_data['gl_web_url']
            }
        )
        database_session.commit()
        
        # Verify the contributor was inserted correctly
        result = database_session.execute(
            text("SELECT * FROM augur_data.contributors WHERE cntrb_id = :cntrb_id"),
            {'cntrb_id': cntrb_id}
        ).fetchone()
        
        assert result is not None
        assert result.gh_login == 'test_github_user_12345'
        assert result.gh_user_id == 999999
        assert result.gl_username is None  # ✅ GitLab column is NULL
        assert result.gl_id is None  # ✅ GitLab column is NULL
    
    def test_insert_gitlab_contributor(
        self,
        database_session,
        cleanup_test_contributors,
        sample_gitlab_contributor_data
    ):
        """Test inserting a GitLab contributor with gl_* columns."""
        # Extract contributor data using the fixed function
        contributor_data = extract_needed_gitlab_contributor_data(sample_gitlab_contributor_data)
        
        # Validate the data
        validate_contributor_data(contributor_data, 'gitlab')
        
        # Insert into database
        cntrb_id = str(uuid.uuid4())
        contributor_data['cntrb_id'] = cntrb_id
        
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login,
                    gh_user_id, gh_login, gh_url,
                    gl_id, gl_username, gl_web_url, gl_avatar_url, gl_state, gl_full_name
                ) VALUES (
                    :cntrb_id, :cntrb_login,
                    :gh_user_id, :gh_login, :gh_url,
                    :gl_id, :gl_username, :gl_web_url, :gl_avatar_url, :gl_state, :gl_full_name
                )
            """),
            {
                'cntrb_id': cntrb_id,
                'cntrb_login': contributor_data['cntrb_login'],
                'gh_user_id': contributor_data['gh_user_id'],
                'gh_login': contributor_data['gh_login'],
                'gh_url': contributor_data['gh_url'],
                'gl_id': contributor_data['gl_id'],
                'gl_username': contributor_data['gl_username'],
                'gl_web_url': contributor_data['gl_web_url'],
                'gl_avatar_url': contributor_data['gl_avatar_url'],
                'gl_state': contributor_data['gl_state'],
                'gl_full_name': contributor_data['gl_full_name']
            }
        )
        database_session.commit()
        
        # Verify the contributor was inserted correctly
        result = database_session.execute(
            text("SELECT * FROM augur_data.contributors WHERE cntrb_id = :cntrb_id"),
            {'cntrb_id': cntrb_id}
        ).fetchone()
        
        assert result is not None
        assert result.gl_username == 'test_gitlab_user_12345'
        assert result.gl_id == 888888
        assert result.gh_login is None  # ✅ GitHub column is NULL
        assert result.gh_user_id is None  # ✅ GitHub column is NULL
    
    def test_same_username_different_forges(
        self,
        database_session,
        cleanup_test_contributors,
        sample_shared_username_github,
        sample_shared_username_gitlab
    ):
        """
        Test inserting GitHub and GitLab users with the same username.
        They should be stored as separate records with different columns populated.
        """
        # Extract data for both users
        github_data = extract_needed_contributor_data(sample_shared_username_github)
        gitlab_data = extract_needed_gitlab_contributor_data(sample_shared_username_gitlab)
        
        # Both have username "shared_username_test" but different columns
        assert github_data['gh_login'] == 'shared_username_test'
        assert gitlab_data['gl_username'] == 'shared_username_test'
        
        # Insert GitHub user
        github_cntrb_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login,
                    gh_user_id, gh_login, gh_url,
                    gl_id, gl_username
                ) VALUES (
                    :cntrb_id, :cntrb_login,
                    :gh_user_id, :gh_login, :gh_url,
                    :gl_id, :gl_username
                )
            """),
            {
                'cntrb_id': github_cntrb_id,
                'cntrb_login': f"gh_{github_data['gh_login']}",  # Prefix to make unique
                'gh_user_id': github_data['gh_user_id'],
                'gh_login': github_data['gh_login'],
                'gh_url': github_data['gh_url'],
                'gl_id': None,
                'gl_username': None
            }
        )
        
        # Insert GitLab user
        gitlab_cntrb_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login,
                    gh_user_id, gh_login,
                    gl_id, gl_username, gl_web_url
                ) VALUES (
                    :cntrb_id, :cntrb_login,
                    :gh_user_id, :gh_login,
                    :gl_id, :gl_username, :gl_web_url
                )
            """),
            {
                'cntrb_id': gitlab_cntrb_id,
                'cntrb_login': f"gl_{gitlab_data['gl_username']}",  # Prefix to make unique
                'gh_user_id': None,
                'gh_login': None,
                'gl_id': gitlab_data['gl_id'],
                'gl_username': gitlab_data['gl_username'],
                'gl_web_url': gitlab_data['gl_web_url']
            }
        )
        database_session.commit()
        
        # Verify both exist in database
        github_result = database_session.execute(
            text("SELECT * FROM augur_data.contributors WHERE cntrb_id = :cntrb_id"),
            {'cntrb_id': github_cntrb_id}
        ).fetchone()
        
        gitlab_result = database_session.execute(
            text("SELECT * FROM augur_data.contributors WHERE cntrb_id = :cntrb_id"),
            {'cntrb_id': gitlab_cntrb_id}
        ).fetchone()
        
        # ✅ Both exist
        assert github_result is not None
        assert gitlab_result is not None
        
        # ✅ GitHub user has gh_login populated, gl_username NULL
        assert github_result.gh_login == 'shared_username_test'
        assert github_result.gh_user_id == 777777
        assert github_result.gl_username is None
        assert github_result.gl_id is None
        
        # ✅ GitLab user has gl_username populated, gh_login NULL
        assert gitlab_result.gl_username == 'shared_username_test'
        assert gitlab_result.gl_id == 666666
        assert gitlab_result.gh_login is None
        assert gitlab_result.gh_user_id is None
        
        # ✅ They are different records
        assert github_result.cntrb_id != gitlab_result.cntrb_id
    
    def test_no_cross_contamination(
        self,
        database_session,
        cleanup_test_contributors,
        sample_github_contributor_data,
        sample_gitlab_contributor_data
    ):
        """
        Verify that no cross-contamination occurs:
        - GitHub data doesn't populate GitLab columns
        - GitLab data doesn't populate GitHub columns
        """
        # Extract and insert both types
        github_data = extract_needed_contributor_data(sample_github_contributor_data)
        gitlab_data = extract_needed_gitlab_contributor_data(sample_gitlab_contributor_data)
        
        # Insert GitHub contributor
        gh_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, :gl_id, :gl_username)
            """),
            {
                'cntrb_id': gh_id,
                'cntrb_login': github_data['cntrb_login'],
                'gh_user_id': github_data['gh_user_id'],
                'gh_login': github_data['gh_login'],
                'gl_id': github_data['gl_id'],
                'gl_username': github_data['gl_username']
            }
        )
        
        # Insert GitLab contributor
        gl_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, :gl_id, :gl_username)
            """),
            {
                'cntrb_id': gl_id,
                'cntrb_login': gitlab_data['cntrb_login'],
                'gh_user_id': gitlab_data['gh_user_id'],
                'gh_login': gitlab_data['gh_login'],
                'gl_id': gitlab_data['gl_id'],
                'gl_username': gitlab_data['gl_username']
            }
        )
        database_session.commit()
        
        # Query and verify no cross-contamination
        results = database_session.execute(
            text("""
                SELECT cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                FROM augur_data.contributors
                WHERE cntrb_id IN (:gh_id, :gl_id)
                ORDER BY gh_user_id DESC NULLS LAST
            """),
            {'gh_id': gh_id, 'gl_id': gl_id}
        ).fetchall()
        
        assert len(results) == 2
        
        # First result should be GitHub user (has gh_user_id)
        gh_row = results[0]
        assert gh_row.gh_user_id is not None
        assert gh_row.gh_login == 'test_github_user_12345'
        assert gh_row.gl_id is None  # ✅ No cross-contamination
        assert gh_row.gl_username is None  # ✅ No cross-contamination
        
        # Second result should be GitLab user (has gl_id)
        gl_row = results[1]
        assert gl_row.gl_id is not None
        assert gl_row.gl_username == 'test_gitlab_user_12345'
        assert gl_row.gh_user_id is None  # ✅ No cross-contamination
        assert gl_row.gh_login is None  # ✅ No cross-contamination
    
    def test_uniqueness_constraints_github(
        self,
        database_session,
        cleanup_test_contributors
    ):
        """
        Test that GH-UNIQUE-C constraint prevents duplicate gh_login values.
        """
        # Insert first GitHub user
        cntrb_id_1 = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, NULL, NULL)
            """),
            {
                'cntrb_id': cntrb_id_1,
                'cntrb_login': 'unique_gh_test_1',
                'gh_user_id': 111111,
                'gh_login': 'octocat_test'
            }
        )
        database_session.commit()
        
        # Try to insert second GitHub user with same gh_login
        cntrb_id_2 = str(uuid.uuid4())
        with pytest.raises(IntegrityError) as exc_info:
            database_session.execute(
                text("""
                    INSERT INTO augur_data.contributors (
                        cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                    ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, NULL, NULL)
                """),
                {
                    'cntrb_id': cntrb_id_2,
                    'cntrb_login': 'unique_gh_test_2',
                    'gh_user_id': 222222,  # Different ID
                    'gh_login': 'octocat_test'  # ❌ Same login
                }
            )
            database_session.commit()
        
        database_session.rollback()
        
        # Verify the constraint error mentions GH-UNIQUE-C
        error_msg = str(exc_info.value)
        assert 'GH-UNIQUE-C' in error_msg or 'gh_login' in error_msg.lower()
    
    def test_uniqueness_constraints_gitlab(
        self,
        database_session,
        cleanup_test_contributors
    ):
        """
        Test that GL-UNIQUE-B and GL-UNIQUE-C constraints prevent duplicate
        gl_id and gl_username values.
        """
        # Insert first GitLab user
        cntrb_id_1 = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, NULL, NULL, :gl_id, :gl_username)
            """),
            {
                'cntrb_id': cntrb_id_1,
                'cntrb_login': 'unique_gl_test_1',
                'gl_id': 333333,
                'gl_username': 'computationalmystic_test'
            }
        )
        database_session.commit()
        
        # Try to insert second GitLab user with same gl_username
        cntrb_id_2 = str(uuid.uuid4())
        with pytest.raises(IntegrityError) as exc_info:
            database_session.execute(
                text("""
                    INSERT INTO augur_data.contributors (
                        cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                    ) VALUES (:cntrb_id, :cntrb_login, NULL, NULL, :gl_id, :gl_username)
                """),
                {
                    'cntrb_id': cntrb_id_2,
                    'cntrb_login': 'unique_gl_test_2',
                    'gl_id': 444444,  # Different ID
                    'gl_username': 'computationalmystic_test'  # ❌ Same username
                }
            )
            database_session.commit()
        
        database_session.rollback()
        
        # Verify the constraint error mentions GL-UNIQUE-C
        error_msg = str(exc_info.value)
        assert 'GL-UNIQUE-C' in error_msg or 'gl_username' in error_msg.lower()
    
    def test_query_by_github_username(
        self,
        database_session,
        cleanup_test_contributors,
        sample_github_contributor_data
    ):
        """Test querying contributors by GitHub username."""
        # Insert GitHub contributor
        github_data = extract_needed_contributor_data(sample_github_contributor_data)
        cntrb_id = str(uuid.uuid4())
        
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gh_url, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, :gh_url, NULL, NULL)
            """),
            {
                'cntrb_id': cntrb_id,
                'cntrb_login': github_data['cntrb_login'],
                'gh_user_id': github_data['gh_user_id'],
                'gh_login': github_data['gh_login'],
                'gh_url': github_data['gh_url']
            }
        )
        database_session.commit()
        
        # Query by gh_login
        result = database_session.execute(
            text("""
                SELECT cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                FROM augur_data.contributors
                WHERE gh_login = :gh_login
            """),
            {'gh_login': 'test_github_user_12345'}
        ).fetchone()
        
        # ✅ Correct data returned
        assert result is not None
        assert result.gh_login == 'test_github_user_12345'
        assert result.gh_user_id == 999999
        assert result.gl_username is None
        assert result.gl_id is None
    
    def test_query_by_gitlab_username(
        self,
        database_session,
        cleanup_test_contributors,
        sample_gitlab_contributor_data
    ):
        """Test querying contributors by GitLab username."""
        # Insert GitLab contributor
        gitlab_data = extract_needed_gitlab_contributor_data(sample_gitlab_contributor_data)
        cntrb_id = str(uuid.uuid4())
        
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username, gl_web_url
                ) VALUES (:cntrb_id, :cntrb_login, NULL, NULL, :gl_id, :gl_username, :gl_web_url)
            """),
            {
                'cntrb_id': cntrb_id,
                'cntrb_login': gitlab_data['cntrb_login'],
                'gl_id': gitlab_data['gl_id'],
                'gl_username': gitlab_data['gl_username'],
                'gl_web_url': gitlab_data['gl_web_url']
            }
        )
        database_session.commit()
        
        # Query by gl_username
        result = database_session.execute(
            text("""
                SELECT cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                FROM augur_data.contributors
                WHERE gl_username = :gl_username
            """),
            {'gl_username': 'test_gitlab_user_12345'}
        ).fetchone()
        
        # ✅ Correct data returned
        assert result is not None
        assert result.gl_username == 'test_gitlab_user_12345'
        assert result.gl_id == 888888
        assert result.gh_login is None
        assert result.gh_user_id is None
    
    def test_complete_workflow_end_to_end(
        self,
        database_session,
        cleanup_test_contributors
    ):
        """
        Complete end-to-end workflow test:
        1. Simulate GitHub issue collection (contributor extraction)
        2. Simulate GitLab merge request collection (contributor extraction)
        3. Verify both are stored correctly
        4. Query each by their respective forge columns
        5. Verify no cross-contamination
        """
        # Step 1: Simulate GitHub issue author collection
        github_issue_author = {
            'login': 'github_workflow_test',
            'id': 555555,
            'url': 'https://api.github.com/users/github_workflow_test',
            'avatar_url': 'https://avatars.githubusercontent.com/u/555555'
        }
        gh_data = extract_needed_contributor_data(github_issue_author)
        validate_contributor_data(gh_data, 'github')
        
        gh_cntrb_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, :gh_user_id, :gh_login, NULL, NULL)
            """),
            {
                'cntrb_id': gh_cntrb_id,
                'cntrb_login': gh_data['cntrb_login'],
                'gh_user_id': gh_data['gh_user_id'],
                'gh_login': gh_data['gh_login']
            }
        )
        
        # Step 2: Simulate GitLab merge request author collection
        gitlab_mr_author = {
            'id': 777888,
            'username': 'gitlab_workflow_test',
            'web_url': 'https://gitlab.com/gitlab_workflow_test',
            'name': 'GitLab Workflow Test',
            'state': 'active'
        }
        gl_data = extract_needed_gitlab_contributor_data(gitlab_mr_author)
        validate_contributor_data(gl_data, 'gitlab')
        
        gl_cntrb_id = str(uuid.uuid4())
        database_session.execute(
            text("""
                INSERT INTO augur_data.contributors (
                    cntrb_id, cntrb_login, gh_user_id, gh_login, gl_id, gl_username
                ) VALUES (:cntrb_id, :cntrb_login, NULL, NULL, :gl_id, :gl_username)
            """),
            {
                'cntrb_id': gl_cntrb_id,
                'cntrb_login': gl_data['cntrb_login'],
                'gl_id': gl_data['gl_id'],
                'gl_username': gl_data['gl_username']
            }
        )
        database_session.commit()
        
        # Step 3: Query for GitHub user
        gh_result = database_session.execute(
            text("""
                SELECT * FROM augur_data.contributors
                WHERE gh_login = :gh_login AND gh_user_id = :gh_user_id
            """),
            {'gh_login': 'github_workflow_test', 'gh_user_id': 555555}
        ).fetchone()
        
        assert gh_result is not None
        assert gh_result.gh_login == 'github_workflow_test'
        assert gh_result.gl_username is None  # ✅ No contamination
        
        # Step 4: Query for GitLab user
        gl_result = database_session.execute(
            text("""
                SELECT * FROM augur_data.contributors
                WHERE gl_username = :gl_username AND gl_id = :gl_id
            """),
            {'gl_username': 'gitlab_workflow_test', 'gl_id': 777888}
        ).fetchone()
        
        assert gl_result is not None
        assert gl_result.gl_username == 'gitlab_workflow_test'
        assert gl_result.gh_login is None  # ✅ No contamination
        
        # Step 5: Verify they are separate records
        assert gh_result.cntrb_id != gl_result.cntrb_id
        
        # Step 6: Verify count
        count_result = database_session.execute(
            text("""
                SELECT COUNT(*) as cnt FROM augur_data.contributors
                WHERE cntrb_login IN ('github_workflow_test', 'gitlab_workflow_test')
            """)
        ).fetchone()
        
        assert count_result.cnt == 2  # ✅ Both records exist


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
