"""
Comprehensive unit tests for GitLab contributor handling fixes (Issue #3469)

This test suite verifies that:
1. GitLab contributor data is stored in gl_* columns (not gh_* columns)
2. GitHub contributor data is stored in gh_* columns (not gl_* columns)
3. Column mapping functions return correct forge-specific mappings
4. Validation functions detect cross-contamination
5. cntrb_login is always populated for both forges
6. Batch processing handles mixed forge data correctly

Related: https://github.com/chaoss/augur/issues/3469
"""

import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the functions we're testing
from augur.tasks.util.contributor_utils import (
    get_contributor_column_mapping,
    get_null_columns_for_other_forges,
    map_contributor_data,
    validate_contributor_data,
    validate_contributor_batch,
    UnsupportedForgeError
)
from augur.application.db.data_parse import (
    extract_needed_contributor_data,
    extract_needed_gitlab_contributor_data
)


# ============================================================================
# Mock API Responses
# ============================================================================

@pytest.fixture
def github_api_user_response():
    """Mock GitHub API user response structure."""
    return {
        'login': 'octocat',
        'id': 583231,
        'node_id': 'MDQ6VXNlcjU4MzIzMQ==',
        'avatar_url': 'https://avatars.githubusercontent.com/u/583231',
        'gravatar_id': '',
        'url': 'https://api.github.com/users/octocat',
        'html_url': 'https://github.com/octocat',
        'followers_url': 'https://api.github.com/users/octocat/followers',
        'following_url': 'https://api.github.com/users/octocat/following{/other_user}',
        'gists_url': 'https://api.github.com/users/octocat/gists{/gist_id}',
        'starred_url': 'https://api.github.com/users/octocat/starred{/owner}{/repo}',
        'subscriptions_url': 'https://api.github.com/users/octocat/subscriptions',
        'organizations_url': 'https://api.github.com/users/octocat/orgs',
        'repos_url': 'https://api.github.com/users/octocat/repos',
        'events_url': 'https://api.github.com/users/octocat/events{/privacy}',
        'received_events_url': 'https://api.github.com/users/octocat/received_events',
        'type': 'User',
        'site_admin': False,
        'name': 'The Octocat',
        'company': '@github',
        'blog': 'https://github.blog',
        'location': 'San Francisco',
        'email': 'octocat@github.com',
        'hireable': None,
        'bio': 'GitHub mascot',
        'created_at': '2011-01-25T18:44:36Z',
        'updated_at': '2023-08-15T12:34:56Z'
    }


@pytest.fixture
def gitlab_api_user_response():
    """Mock GitLab API user response structure."""
    return {
        'id': 5481034,
        'username': 'computationalmystic',
        'name': 'Sean Goggins',
        'state': 'active',
        'avatar_url': 'https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon',
        'web_url': 'https://gitlab.com/computationalmystic',
        'created_at': '2019-03-15T14:23:45.123Z',
        'bio': 'Professor studying open source',
        'location': 'Missouri, USA',
        'public_email': 's@goggins.com',
        'skype': '',
        'linkedin': '',
        'twitter': '',
        'website_url': 'http://www.seangoggins.net',
        'organization': 'University of Missouri',
        'job_title': 'Associate Professor',
        'work_information': None
    }


@pytest.fixture
def mock_logger():
    """Mock logger for testing validation warnings."""
    return Mock()


# ============================================================================
# Test 1: Column Mapping for GitHub
# ============================================================================

class TestGetContributorColumnMappingGitHub:
    """Test that GitHub column mapping returns correct gh_* columns."""
    
    def test_returns_github_columns(self):
        """Verify GitHub mapping returns gh_* columns."""
        mapping = get_contributor_column_mapping('github')
        
        # Check that all expected GitHub columns are present
        assert 'user_id' in mapping
        assert mapping['user_id'] == 'gh_user_id'
        
        assert 'login' in mapping
        assert mapping['login'] == 'gh_login'
        
        assert 'url' in mapping
        assert mapping['url'] == 'gh_url'
        
        assert 'avatar_url' in mapping
        assert mapping['avatar_url'] == 'gh_avatar_url'
        
        assert 'html_url' in mapping
        assert mapping['html_url'] == 'gh_html_url'
    
    def test_does_not_return_gitlab_columns(self):
        """Verify GitHub mapping does NOT return gl_* columns."""
        mapping = get_contributor_column_mapping('github')
        
        # Make sure no GitLab-specific columns are in the mapping
        for key, value in mapping.items():
            assert not value.startswith('gl_'), f"GitHub mapping should not contain GitLab column: {value}"
    
    def test_includes_all_18_github_columns(self):
        """Verify all 18 GitHub columns are mapped."""
        mapping = get_contributor_column_mapping('github')
        
        expected_columns = [
            'gh_user_id', 'gh_login', 'gh_url', 'gh_html_url', 'gh_node_id',
            'gh_avatar_url', 'gh_gravatar_id', 'gh_followers_url', 'gh_following_url',
            'gh_gists_url', 'gh_starred_url', 'gh_subscriptions_url',
            'gh_organizations_url', 'gh_repos_url', 'gh_events_url',
            'gh_received_events_url', 'gh_type', 'gh_site_admin'
        ]
        
        # All mapped values should be GitHub columns
        mapped_values = list(mapping.values())
        for col in expected_columns:
            assert col in mapped_values, f"Missing GitHub column in mapping: {col}"


# ============================================================================
# Test 2: Column Mapping for GitLab
# ============================================================================

class TestGetContributorColumnMappingGitLab:
    """Test that GitLab column mapping returns correct gl_* columns."""
    
    def test_returns_gitlab_columns(self):
        """Verify GitLab mapping returns gl_* columns."""
        mapping = get_contributor_column_mapping('gitlab')
        
        # Check that all expected GitLab columns are present
        assert 'user_id' in mapping
        assert mapping['user_id'] == 'gl_id'
        
        assert 'login' in mapping
        assert mapping['login'] == 'gl_username'
        
        assert 'url' in mapping
        assert mapping['url'] == 'gl_web_url'
        
        assert 'avatar_url' in mapping
        assert mapping['avatar_url'] == 'gl_avatar_url'
    
    def test_does_not_return_github_columns(self):
        """Verify GitLab mapping does NOT return gh_* columns."""
        mapping = get_contributor_column_mapping('gitlab')
        
        # Make sure no GitHub-specific columns are in the mapping
        for key, value in mapping.items():
            assert not value.startswith('gh_'), f"GitLab mapping should not contain GitHub column: {value}"
    
    def test_includes_all_6_gitlab_columns(self):
        """Verify all 6 GitLab columns are mapped."""
        mapping = get_contributor_column_mapping('gitlab')
        
        expected_columns = [
            'gl_id', 'gl_username', 'gl_web_url', 'gl_avatar_url',
            'gl_state', 'gl_full_name'
        ]
        
        # All mapped values should be GitLab columns
        mapped_values = list(mapping.values())
        for col in expected_columns:
            assert col in mapped_values, f"Missing GitLab column in mapping: {col}"
    
    def test_gitlab_specific_fields_present(self):
        """Verify GitLab-specific fields like state and full_name are mapped."""
        mapping = get_contributor_column_mapping('gitlab')
        
        assert 'state' in mapping
        assert mapping['state'] == 'gl_state'
        
        assert 'full_name' in mapping
        assert mapping['full_name'] == 'gl_full_name'


# ============================================================================
# Test 3: GitLab Data Uses Correct Columns
# ============================================================================

class TestGitLabDataUsesCorrectColumns:
    """Test that GitLab API data is stored in gl_* columns."""
    
    def test_extract_gitlab_contributor_populates_gl_columns(self, gitlab_api_user_response):
        """Verify extract_needed_gitlab_contributor_data uses gl_* columns."""
        result = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        # GitLab data should be in gl_* columns
        assert result['gl_id'] == 5481034
        assert result['gl_username'] == 'computationalmystic'
        assert result['gl_web_url'] == 'https://gitlab.com/computationalmystic'
        assert result['gl_full_name'] == 'Sean Goggins'
        assert result['gl_state'] == 'active'
        assert 'secure.gravatar.com' in result['gl_avatar_url']
    
    def test_extract_gitlab_contributor_nulls_gh_columns(self, gitlab_api_user_response):
        """Verify extract_needed_gitlab_contributor_data sets gh_* columns to NULL."""
        result = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        # All GitHub columns should be None
        github_columns = [
            'gh_user_id', 'gh_login', 'gh_url', 'gh_html_url', 'gh_node_id',
            'gh_avatar_url', 'gh_gravatar_id', 'gh_followers_url', 'gh_following_url',
            'gh_gists_url', 'gh_starred_url', 'gh_subscriptions_url',
            'gh_organizations_url', 'gh_repos_url', 'gh_events_url',
            'gh_received_events_url', 'gh_type', 'gh_site_admin'
        ]
        
        for col in github_columns:
            assert result[col] is None, f"GitHub column {col} should be NULL for GitLab data"
    
    def test_cntrb_login_populated_from_gitlab_username(self, gitlab_api_user_response):
        """Verify cntrb_login is populated from GitLab username."""
        result = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        assert result['cntrb_login'] == 'computationalmystic'
        assert result['cntrb_login'] == result['gl_username']
    
    def test_gitlab_user_without_optional_fields(self):
        """Test GitLab user with minimal data (only required fields)."""
        minimal_gitlab_user = {
            'id': 12345,
            'username': 'minimal_user',
            'web_url': 'https://gitlab.com/minimal_user'
        }
        
        result = extract_needed_gitlab_contributor_data(minimal_gitlab_user)
        
        # Required fields should be present
        assert result['gl_id'] == 12345
        assert result['gl_username'] == 'minimal_user'
        assert result['gl_web_url'] == 'https://gitlab.com/minimal_user'
        
        # Optional fields can be None
        assert result.get('gl_full_name') is None or result.get('gl_full_name') == ''
        assert result.get('gl_state') is None or result.get('gl_state') == ''
        
        # GitHub columns still NULL
        assert result['gh_user_id'] is None
        assert result['gh_login'] is None
    
    def test_map_contributor_data_for_gitlab(self, gitlab_api_user_response):
        """Test the convenience map_contributor_data function for GitLab."""
        result = map_contributor_data(gitlab_api_user_response, 'gitlab')
        
        # Should map to gl_* columns
        assert result['gl_id'] == 5481034
        assert result['gl_username'] == 'computationalmystic'
        
        # Should null out gh_* columns
        assert result['gh_user_id'] is None
        assert result['gh_login'] is None


# ============================================================================
# Test 4: GitHub Data Uses Correct Columns
# ============================================================================

class TestGitHubDataUsesCorrectColumns:
    """Test that GitHub API data is stored in gh_* columns."""
    
    def test_extract_github_contributor_populates_gh_columns(self, github_api_user_response):
        """Verify extract_needed_contributor_data uses gh_* columns."""
        result = extract_needed_contributor_data(github_api_user_response)
        
        # GitHub data should be in gh_* columns
        assert result['gh_user_id'] == 583231
        assert result['gh_login'] == 'octocat'
        assert result['gh_url'] == 'https://api.github.com/users/octocat'
        assert result['gh_html_url'] == 'https://github.com/octocat'
        assert result['gh_node_id'] == 'MDQ6VXNlcjU4MzIzMQ=='
        assert result['gh_avatar_url'] == 'https://avatars.githubusercontent.com/u/583231'
        assert result['gh_type'] == 'User'
        assert result['gh_site_admin'] is False
    
    def test_extract_github_contributor_nulls_gl_columns(self, github_api_user_response):
        """Verify extract_needed_contributor_data sets gl_* columns to NULL."""
        result = extract_needed_contributor_data(github_api_user_response)
        
        # All GitLab columns should be None
        gitlab_columns = [
            'gl_id', 'gl_username', 'gl_web_url', 'gl_avatar_url',
            'gl_state', 'gl_full_name'
        ]
        
        for col in gitlab_columns:
            assert result[col] is None, f"GitLab column {col} should be NULL for GitHub data"
    
    def test_cntrb_login_populated_from_github_login(self, github_api_user_response):
        """Verify cntrb_login is populated from GitHub login."""
        result = extract_needed_contributor_data(github_api_user_response)
        
        assert result['cntrb_login'] == 'octocat'
        assert result['cntrb_login'] == result['gh_login']
    
    def test_github_user_with_all_fields(self, github_api_user_response):
        """Test GitHub user with complete data."""
        result = extract_needed_contributor_data(github_api_user_response)
        
        # Check all URL fields are populated
        assert result['gh_followers_url'] == 'https://api.github.com/users/octocat/followers'
        assert result['gh_repos_url'] == 'https://api.github.com/users/octocat/repos'
        assert result['gh_organizations_url'] == 'https://api.github.com/users/octocat/orgs'
        assert result['gh_gists_url'].startswith('https://api.github.com/users/octocat/gists')
        
        # cntrb fields should also be populated
        assert result['cntrb_full_name'] == 'The Octocat'
        assert result['cntrb_company'] == '@github'
        assert result['cntrb_location'] == 'San Francisco'
        assert result['cntrb_email'] == 'octocat@github.com'
    
    def test_map_contributor_data_for_github(self, github_api_user_response):
        """Test the convenience map_contributor_data function for GitHub."""
        result = map_contributor_data(github_api_user_response, 'github')
        
        # Should map to gh_* columns
        assert result['gh_user_id'] == 583231
        assert result['gh_login'] == 'octocat'
        
        # Should null out gl_* columns
        assert result['gl_id'] is None
        assert result['gl_username'] is None


# ============================================================================
# Test 5: Validation Rejects Mixed Data
# ============================================================================

class TestValidateContributorDataRejectsMixedData:
    """Test that validation catches cross-contamination."""
    
    def test_rejects_gitlab_user_with_gh_columns_populated(self, mock_logger):
        """Validation should fail if GitLab user has gh_* columns populated."""
        # This simulates the bug from issue #3469
        contaminated_data = {
            'cntrb_login': 'computationalmystic',
            'gh_user_id': 5481034,  # ❌ GitLab ID in GitHub column!
            'gh_login': 'computationalmystic',  # ❌ GitLab username in GitHub column!
            'gh_url': 'https://gitlab.com/computationalmystic',  # ❌ GitLab URL!
            'gl_id': None,
            'gl_username': None,
            'gl_web_url': None
        }
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_data(contaminated_data, 'gitlab', mock_logger)
        
        error_msg = str(exc_info.value)
        assert 'cross-contamination' in error_msg.lower()
        assert 'gh_user_id' in error_msg
        assert 'gh_login' in error_msg
        assert 'gh_url' in error_msg
        assert '#3469' in error_msg
    
    def test_rejects_github_user_with_gl_columns_populated(self, mock_logger):
        """Validation should fail if GitHub user has gl_* columns populated."""
        contaminated_data = {
            'cntrb_login': 'octocat',
            'gl_id': 583231,  # ❌ GitHub ID in GitLab column!
            'gl_username': 'octocat',  # ❌ GitHub username in GitLab column!
            'gl_web_url': 'https://github.com/octocat',  # ❌ GitHub URL!
            'gh_user_id': None,
            'gh_login': None,
            'gh_url': None
        }
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_data(contaminated_data, 'github', mock_logger)
        
        error_msg = str(exc_info.value)
        assert 'cross-contamination' in error_msg.lower()
        assert 'gl_id' in error_msg
        assert 'gl_username' in error_msg
        assert 'gl_web_url' in error_msg
    
    def test_accepts_valid_gitlab_data(self, gitlab_api_user_response, mock_logger):
        """Validation should pass for properly formatted GitLab data."""
        gitlab_data = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        # Should not raise any exception
        validate_contributor_data(gitlab_data, 'gitlab', mock_logger)
    
    def test_accepts_valid_github_data(self, github_api_user_response, mock_logger):
        """Validation should pass for properly formatted GitHub data."""
        github_data = extract_needed_contributor_data(github_api_user_response)
        
        # Should not raise any exception
        validate_contributor_data(github_data, 'github', mock_logger)
    
    def test_warns_about_missing_critical_fields(self, mock_logger):
        """Validation should warn if critical fields are missing."""
        incomplete_data = {
            'cntrb_login': 'user',
            'gl_web_url': 'https://gitlab.com/user',  # Has URL but missing ID/username
            'gl_id': None,  # ⚠️ Missing
            'gl_username': None,  # ⚠️ Missing
            'gh_user_id': None,
            'gh_login': None
        }
        
        # Should not raise, but should log warnings
        validate_contributor_data(incomplete_data, 'gitlab', mock_logger)
        
        # Check that warnings were logged
        assert mock_logger.warning.called
        warning_messages = ' '.join([str(call[0][0]) for call in mock_logger.warning.call_args_list])
        assert 'gl_id' in warning_messages or 'gl_username' in warning_messages


# ============================================================================
# Test 6: cntrb_login Always Populated
# ============================================================================

class TestCntrbLoginAlwaysPopulated:
    """Test that cntrb_login is always populated for both forges."""
    
    def test_github_contributor_has_cntrb_login(self, github_api_user_response):
        """GitHub contributors should have cntrb_login populated."""
        result = extract_needed_contributor_data(github_api_user_response)
        
        assert result['cntrb_login'] is not None
        assert result['cntrb_login'] != ''
        assert result['cntrb_login'] == 'octocat'
        assert result['cntrb_login'] == result['gh_login']
    
    def test_gitlab_contributor_has_cntrb_login(self, gitlab_api_user_response):
        """GitLab contributors should have cntrb_login populated."""
        result = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        assert result['cntrb_login'] is not None
        assert result['cntrb_login'] != ''
        assert result['cntrb_login'] == 'computationalmystic'
        assert result['cntrb_login'] == result['gl_username']
    
    def test_cntrb_login_unique_constraint_purpose(self):
        """Document why cntrb_login exists and should be unique."""
        # cntrb_login serves as the universal login field across all forges
        # It should match gh_login for GitHub users and gl_username for GitLab users
        # The GL-cntrb-LOGIN-UNIQUE constraint ensures no duplicate logins
        
        github_user = {'login': 'testuser', 'id': 123}
        gitlab_user = {'username': 'testuser', 'id': 456, 'web_url': 'https://gitlab.com/testuser'}
        
        gh_result = extract_needed_contributor_data(github_user)
        gl_result = extract_needed_gitlab_contributor_data(gitlab_user)
        
        # Both should have cntrb_login populated
        assert gh_result['cntrb_login'] == 'testuser'
        assert gl_result['cntrb_login'] == 'testuser'
        
        # But they should be distinguishable by their forge-specific columns
        assert gh_result['gh_user_id'] == 123
        assert gh_result['gl_id'] is None
        
        assert gl_result['gl_id'] == 456
        assert gl_result['gh_user_id'] is None
    
    def test_validation_warns_on_missing_cntrb_login(self, mock_logger):
        """Validation should warn if cntrb_login is missing or empty."""
        data_without_login = {
            'gh_user_id': 123,
            'gh_login': 'testuser',
            'gl_id': None,
            # cntrb_login missing!
        }
        
        validate_contributor_data(data_without_login, 'github', mock_logger)
        
        # Should have logged a warning
        assert mock_logger.warning.called
        warning_messages = ' '.join([str(call[0][0]) for call in mock_logger.warning.call_args_list])
        assert 'cntrb_login' in warning_messages


# ============================================================================
# Test 7: No Cross-Contamination in Batch Processing
# ============================================================================

class TestNoCrossContaminationInBatchProcessing:
    """Test that mixed GitHub and GitLab data is handled correctly."""
    
    def test_batch_with_mixed_github_and_gitlab_users(
        self, 
        github_api_user_response, 
        gitlab_api_user_response,
        mock_logger
    ):
        """Process a batch with both GitHub and GitLab users."""
        # Extract data for both users
        github_data = extract_needed_contributor_data(github_api_user_response)
        gitlab_data = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        
        # Verify GitHub user uses gh_* columns
        assert github_data['gh_user_id'] == 583231
        assert github_data['gh_login'] == 'octocat'
        assert github_data['gl_id'] is None
        assert github_data['gl_username'] is None
        
        # Verify GitLab user uses gl_* columns
        assert gitlab_data['gl_id'] == 5481034
        assert gitlab_data['gl_username'] == 'computationalmystic'
        assert gitlab_data['gh_user_id'] is None
        assert gitlab_data['gh_login'] is None
        
        # Validate both (should not raise)
        validate_contributor_data(github_data, 'github', mock_logger)
        validate_contributor_data(gitlab_data, 'gitlab', mock_logger)
    
    def test_validate_contributor_batch_with_mixed_data(
        self,
        github_api_user_response,
        gitlab_api_user_response,
        mock_logger
    ):
        """Test batch validation with mixed forge data."""
        # Create a batch of GitHub contributors
        github_batch = [
            extract_needed_contributor_data(github_api_user_response),
            extract_needed_contributor_data({
                'login': 'another_gh_user',
                'id': 999999,
                'url': 'https://api.github.com/users/another_gh_user'
            })
        ]
        
        # Create a batch of GitLab contributors
        gitlab_batch = [
            extract_needed_gitlab_contributor_data(gitlab_api_user_response),
            extract_needed_gitlab_contributor_data({
                'username': 'another_gl_user',
                'id': 888888,
                'web_url': 'https://gitlab.com/another_gl_user'
            })
        ]
        
        # Validate each batch with correct forge type (should not raise)
        validate_contributor_batch(github_batch, 'github', mock_logger)
        validate_contributor_batch(gitlab_batch, 'gitlab', mock_logger)
    
    def test_batch_validation_catches_mixed_contamination(self, mock_logger):
        """Test that batch validation catches multiple contaminated records."""
        contaminated_batch = [
            {
                'cntrb_login': 'user1',
                'gh_user_id': 123,  # ❌ Wrong for GitLab
                'gl_id': None,
                'gl_username': None
            },
            {
                'cntrb_login': 'user2',
                'gh_login': 'user2',  # ❌ Wrong for GitLab
                'gl_id': None,
                'gl_username': None
            },
            {
                'cntrb_login': 'user3',
                'gl_id': 789,  # ✅ Correct
                'gl_username': 'user3',  # ✅ Correct
                'gh_user_id': None,
                'gh_login': None
            }
        ]
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_batch(contaminated_batch, 'gitlab', mock_logger)
        
        error_msg = str(exc_info.value)
        assert 'Contributor #0' in error_msg  # user1 failed
        assert 'Contributor #1' in error_msg  # user2 failed
        assert '2 out of 3' in error_msg
    
    def test_processing_github_issues_and_gitlab_issues_separately(
        self,
        github_api_user_response,
        gitlab_api_user_response
    ):
        """
        Simulate real-world scenario: processing GitHub issues and GitLab issues
        in the same Augur instance.
        """
        # Simulate GitHub issue collection
        github_issue_author = extract_needed_contributor_data(github_api_user_response)
        assert github_issue_author['gh_login'] == 'octocat'
        assert github_issue_author['gl_username'] is None
        
        # Simulate GitLab issue collection
        gitlab_issue_author = extract_needed_gitlab_contributor_data(gitlab_api_user_response)
        assert gitlab_issue_author['gl_username'] == 'computationalmystic'
        assert gitlab_issue_author['gh_login'] is None
        
        # Both should have different cntrb_login values
        assert github_issue_author['cntrb_login'] != gitlab_issue_author['cntrb_login']
        
        # Both should be valid for insertion into the same contributors table
        # (The database will use different columns for each)


# ============================================================================
# Additional Edge Case Tests
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_unsupported_forge_type_raises_error(self):
        """Test that unsupported forge type raises UnsupportedForgeError."""
        with pytest.raises(UnsupportedForgeError):
            get_contributor_column_mapping('bitbucket')
        
        with pytest.raises(UnsupportedForgeError):
            validate_contributor_data({'cntrb_login': 'user'}, 'unsupported_forge')
    
    def test_null_values_handled_correctly(self):
        """Test that None/NULL values are handled correctly in validation."""
        data_with_nulls = {
            'cntrb_login': 'testuser',
            'gh_user_id': None,
            'gh_login': None,
            'gh_url': None,
            'gh_avatar_url': None,
            'gl_id': 12345,
            'gl_username': 'testuser',
            'gl_web_url': 'https://gitlab.com/testuser',
            'gl_avatar_url': None,  # NULL is OK for optional fields
            'gl_state': None,
            'gl_full_name': None
        }
        
        # Should not raise - NULLs in optional fields are fine
        validate_contributor_data(data_with_nulls, 'gitlab')
    
    def test_empty_batch_validation(self):
        """Test that empty batch doesn't cause errors."""
        # Should not raise
        validate_contributor_batch([], 'github')
        validate_contributor_batch([], 'gitlab')
    
    def test_case_insensitive_forge_type(self):
        """Test that forge type is case-insensitive."""
        # These should all work
        mapping_lower = get_contributor_column_mapping('github')
        mapping_upper = get_contributor_column_mapping('GITHUB')
        mapping_mixed = get_contributor_column_mapping('GitHub')
        
        assert mapping_lower == mapping_upper == mapping_mixed
    
    def test_gitlab_user_with_deactivated_state(self):
        """Test GitLab user with 'blocked' or 'deactivated' state."""
        blocked_user = {
            'id': 11111,
            'username': 'blocked_user',
            'state': 'blocked',
            'web_url': 'https://gitlab.com/blocked_user',
            'avatar_url': 'https://gitlab.com/avatar.png',
            'name': 'Blocked User'
        }
        
        result = extract_needed_gitlab_contributor_data(blocked_user)
        
        # Should still extract the data
        assert result['gl_id'] == 11111
        assert result['gl_username'] == 'blocked_user'
        assert result['gl_state'] == 'blocked'
        
        # Should still null out GitHub columns
        assert result['gh_user_id'] is None
        assert result['gh_login'] is None


# ============================================================================
# Integration Test Simulation
# ============================================================================

class TestIntegrationScenario:
    """
    Simulate real-world integration scenarios where the bug from issue #3469
    would have manifested.
    """
    
    def test_issue_3469_bug_scenario(self):
        """
        Recreate the exact bug scenario from issue #3469:
        GitLab username was stored in gh_login, causing constraint violation.
        """
        # Old buggy behavior (before fix)
        # This is what the old code did wrong:
        buggy_gitlab_extraction = {
            'cntrb_login': 'computationalmystic',
            'gh_user_id': 5481034,  # ❌ Bug: GitLab ID in GitHub column
            'gh_login': 'computationalmystic',  # ❌ Bug: GitLab username in gh_login
            'gh_url': 'https://gitlab.com/computationalmystic',  # ❌ Bug
            'gl_id': None,  # ❌ Bug: Should be 5481034
            'gl_username': None  # ❌ Bug: Should be 'computationalmystic'
        }
        
        # This would violate the GH-UNIQUE-C constraint and cause cross-contamination
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_data(buggy_gitlab_extraction, 'gitlab')
        
        assert '#3469' in str(exc_info.value)
        
        # New correct behavior (after fix)
        correct_gitlab_extraction = {
            'cntrb_login': 'computationalmystic',
            'gh_user_id': None,  # ✅ Correct: NULL for GitLab user
            'gh_login': None,  # ✅ Correct: NULL for GitLab user
            'gh_url': None,  # ✅ Correct: NULL for GitLab user
            'gl_id': 5481034,  # ✅ Correct: GitLab ID in gl_id
            'gl_username': 'computationalmystic',  # ✅ Correct: Username in gl_username
            'gl_web_url': 'https://gitlab.com/computationalmystic'  # ✅ Correct
        }
        
        # This should pass validation
        validate_contributor_data(correct_gitlab_extraction, 'gitlab')
    
    def test_multiple_gitlab_repos_no_duplicates(self):
        """
        Test that collecting from multiple GitLab repos doesn't cause
        duplicate entries or cross-contamination.
        """
        # Same user appears in multiple GitLab repos
        user_from_repo1 = extract_needed_gitlab_contributor_data({
            'id': 5481034,
            'username': 'computationalmystic',
            'web_url': 'https://gitlab.com/computationalmystic',
            'name': 'Sean Goggins'
        })
        
        user_from_repo2 = extract_needed_gitlab_contributor_data({
            'id': 5481034,  # Same ID
            'username': 'computationalmystic',  # Same username
            'web_url': 'https://gitlab.com/computationalmystic',
            'name': 'Sean Goggins'
        })
        
        # Both extractions should produce identical data
        assert user_from_repo1['gl_id'] == user_from_repo2['gl_id']
        assert user_from_repo1['gl_username'] == user_from_repo2['gl_username']
        assert user_from_repo1['cntrb_login'] == user_from_repo2['cntrb_login']
        
        # Both should have NULL GitHub columns
        assert user_from_repo1['gh_login'] is None
        assert user_from_repo2['gh_login'] is None
        
        # The database UNIQUE constraint on gl_id should prevent duplicates
        # (This is tested in the migration tests)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
