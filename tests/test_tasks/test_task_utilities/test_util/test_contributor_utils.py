"""
Unit tests for contributor_utils module.

Tests the forge-specific column mapping functions to ensure correct routing
of contributor data to appropriate database columns.
"""

import pytest
from augur.tasks.util.contributor_utils import (
    get_contributor_column_mapping,
    get_null_columns_for_other_forges,
    map_contributor_data,
    get_supported_forge_types,
    validate_forge_type,
    UnsupportedForgeError
)


class TestGetContributorColumnMapping:
    """Tests for get_contributor_column_mapping function."""
    
    def test_github_mapping(self):
        """Test that GitHub mapping returns correct column names."""
        mapping = get_contributor_column_mapping("github")
        
        # Test primary fields
        assert mapping['user_id'] == 'gh_user_id'
        assert mapping['login'] == 'gh_login'
        assert mapping['url'] == 'gh_url'
        assert mapping['avatar_url'] == 'gh_avatar_url'
        
        # Test GitHub-specific fields
        assert mapping['node_id'] == 'gh_node_id'
        assert mapping['type'] == 'gh_type'
        assert mapping['site_admin'] == 'gh_site_admin'
        
        # GitLab-specific field should be None
        assert mapping['state'] is None
    
    def test_gitlab_mapping(self):
        """Test that GitLab mapping returns correct column names."""
        mapping = get_contributor_column_mapping("gitlab")
        
        # Test primary fields
        assert mapping['user_id'] == 'gl_id'
        assert mapping['id'] == 'gl_id'
        assert mapping['login'] == 'gl_username'
        assert mapping['username'] == 'gl_username'
        assert mapping['url'] == 'gl_web_url'
        assert mapping['web_url'] == 'gl_web_url'
        assert mapping['avatar_url'] == 'gl_avatar_url'
        
        # Test GitLab-specific fields
        assert mapping['state'] == 'gl_state'
        assert mapping['full_name'] == 'gl_full_name'
        assert mapping['name'] == 'gl_full_name'
        
        # GitHub-specific fields should be None
        assert mapping['node_id'] is None
        assert mapping['type'] is None
        assert mapping['site_admin'] is None
    
    def test_case_insensitive(self):
        """Test that forge type is case-insensitive."""
        mapping1 = get_contributor_column_mapping("GitHub")
        mapping2 = get_contributor_column_mapping("GITHUB")
        mapping3 = get_contributor_column_mapping("github")
        
        assert mapping1 == mapping2 == mapping3
        assert mapping1['user_id'] == 'gh_user_id'
    
    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        mapping = get_contributor_column_mapping("  gitlab  ")
        assert mapping['user_id'] == 'gl_id'
    
    def test_unsupported_forge_type(self):
        """Test that unsupported forge types raise UnsupportedForgeError."""
        with pytest.raises(UnsupportedForgeError) as exc_info:
            get_contributor_column_mapping("bitbucket")
        
        assert "bitbucket" in str(exc_info.value).lower()
        assert "github" in str(exc_info.value).lower()
        assert "gitlab" in str(exc_info.value).lower()
    
    def test_empty_forge_type(self):
        """Test that empty forge type raises UnsupportedForgeError."""
        with pytest.raises(UnsupportedForgeError):
            get_contributor_column_mapping("")


class TestGetNullColumnsForOtherForges:
    """Tests for get_null_columns_for_other_forges function."""
    
    def test_github_null_columns(self):
        """Test that GitLab columns are returned as None for GitHub."""
        null_cols = get_null_columns_for_other_forges("github")
        
        # Should contain all GitLab columns
        assert 'gl_id' in null_cols
        assert 'gl_username' in null_cols
        assert 'gl_web_url' in null_cols
        assert 'gl_avatar_url' in null_cols
        assert 'gl_state' in null_cols
        assert 'gl_full_name' in null_cols
        
        # All should be None
        for value in null_cols.values():
            assert value is None
        
        # Should NOT contain GitHub columns
        assert 'gh_user_id' not in null_cols
        assert 'gh_login' not in null_cols
    
    def test_gitlab_null_columns(self):
        """Test that GitHub columns are returned as None for GitLab."""
        null_cols = get_null_columns_for_other_forges("gitlab")
        
        # Should contain all GitHub columns
        assert 'gh_user_id' in null_cols
        assert 'gh_login' in null_cols
        assert 'gh_url' in null_cols
        assert 'gh_avatar_url' in null_cols
        assert 'gh_node_id' in null_cols
        assert 'gh_type' in null_cols
        
        # All should be None
        for value in null_cols.values():
            assert value is None
        
        # Should NOT contain GitLab columns
        assert 'gl_id' not in null_cols
        assert 'gl_username' not in null_cols
    
    def test_unsupported_forge_type(self):
        """Test that unsupported forge types raise UnsupportedForgeError."""
        with pytest.raises(UnsupportedForgeError):
            get_null_columns_for_other_forges("bitbucket")


class TestMapContributorData:
    """Tests for map_contributor_data function."""
    
    def test_github_data_mapping(self):
        """Test mapping of GitHub contributor data."""
        github_data = {
            'id': 12345,
            'login': 'octocat',
            'url': 'https://api.github.com/users/octocat',
            'avatar_url': 'https://avatars.githubusercontent.com/u/12345',
            'node_id': 'MDQ6VXNlcjEyMzQ1',
            'type': 'User',
            'site_admin': False
        }
        
        result = map_contributor_data(github_data, "github")
        
        # Check GitHub columns are populated
        assert result['gh_user_id'] == 12345
        assert result['gh_login'] == 'octocat'
        assert result['gh_url'] == 'https://api.github.com/users/octocat'
        assert result['gh_avatar_url'] == 'https://avatars.githubusercontent.com/u/12345'
        assert result['gh_node_id'] == 'MDQ6VXNlcjEyMzQ1'
        assert result['gh_type'] == 'User'
        assert result['gh_site_admin'] == False
        
        # Check GitLab columns are None
        assert result['gl_id'] is None
        assert result['gl_username'] is None
        assert result['gl_web_url'] is None
        assert result['gl_avatar_url'] is None
    
    def test_gitlab_data_mapping(self):
        """Test mapping of GitLab contributor data."""
        gitlab_data = {
            'id': 67890,
            'username': 'johndoe',
            'web_url': 'https://gitlab.com/johndoe',
            'avatar_url': 'https://secure.gravatar.com/avatar/abc123',
            'state': 'active',
            'name': 'John Doe'
        }
        
        result = map_contributor_data(gitlab_data, "gitlab")
        
        # Check GitLab columns are populated
        assert result['gl_id'] == 67890
        assert result['gl_username'] == 'johndoe'
        assert result['gl_web_url'] == 'https://gitlab.com/johndoe'
        assert result['gl_avatar_url'] == 'https://secure.gravatar.com/avatar/abc123'
        assert result['gl_state'] == 'active'
        assert result['gl_full_name'] == 'John Doe'
        
        # Check GitHub columns are None
        assert result['gh_user_id'] is None
        assert result['gh_login'] is None
        assert result['gh_url'] is None
        assert result['gh_avatar_url'] is None
        assert result['gh_node_id'] is None
        assert result['gh_type'] is None
    
    def test_missing_optional_fields(self):
        """Test that missing fields are handled gracefully."""
        minimal_data = {
            'id': 123,
            'username': 'minimal'
        }
        
        result = map_contributor_data(minimal_data, "gitlab")
        
        # Required fields should be present
        assert result['gl_id'] == 123
        assert result['gl_username'] == 'minimal'
        
        # Optional fields not in input shouldn't be in output
        assert 'gl_avatar_url' not in result or result.get('gl_avatar_url') is None
        
        # Other forge columns should still be None
        assert result['gh_user_id'] is None


class TestGetSupportedForgeTypes:
    """Tests for get_supported_forge_types function."""
    
    def test_returns_list(self):
        """Test that function returns a list."""
        result = get_supported_forge_types()
        assert isinstance(result, list)
    
    def test_contains_known_forges(self):
        """Test that result contains known forge types."""
        result = get_supported_forge_types()
        assert "github" in result
        assert "gitlab" in result
    
    def test_all_lowercase(self):
        """Test that all forge types are lowercase."""
        result = get_supported_forge_types()
        for forge in result:
            assert forge == forge.lower()


class TestValidateForgeType:
    """Tests for validate_forge_type function."""
    
    def test_valid_github(self):
        """Test that 'github' is validated correctly."""
        assert validate_forge_type("github") is True
        assert validate_forge_type("GitHub") is True
        assert validate_forge_type("GITHUB") is True
    
    def test_valid_gitlab(self):
        """Test that 'gitlab' is validated correctly."""
        assert validate_forge_type("gitlab") is True
        assert validate_forge_type("GitLab") is True
        assert validate_forge_type("GITLAB") is True
    
    def test_invalid_forge(self):
        """Test that invalid forge types return False."""
        assert validate_forge_type("bitbucket") is False
        assert validate_forge_type("sourceforge") is False
        assert validate_forge_type("") is False
        assert validate_forge_type("unknown") is False
    
    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        assert validate_forge_type("  github  ") is True
        assert validate_forge_type("  gitlab  ") is True


class TestRealWorldScenarios:
    """Integration tests with real-world data scenarios."""
    
    def test_github_issue_author(self):
        """Test with GitHub issue author data structure."""
        github_author = {
            'login': 'octocat',
            'id': 583231,
            'node_id': 'MDQ6VXNlcjU4MzIzMQ==',
            'avatar_url': 'https://avatars.githubusercontent.com/u/583231',
            'url': 'https://api.github.com/users/octocat',
            'html_url': 'https://github.com/octocat',
            'type': 'User',
            'site_admin': False
        }
        
        result = map_contributor_data(github_author, "github")
        
        assert result['gh_user_id'] == 583231
        assert result['gh_login'] == 'octocat'
        assert result['gh_node_id'] == 'MDQ6VXNlcjU4MzIzMQ=='
        assert result['gl_id'] is None
    
    def test_gitlab_merge_request_author(self):
        """Test with GitLab merge request author data structure."""
        gitlab_author = {
            'id': 5481034,
            'username': 'computationalmystic',
            'name': 'Sean Goggins',
            'state': 'active',
            'avatar_url': 'https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c',
            'web_url': 'https://gitlab.com/computationalmystic'
        }
        
        result = map_contributor_data(gitlab_author, "gitlab")
        
        assert result['gl_id'] == 5481034
        assert result['gl_username'] == 'computationalmystic'
        assert result['gl_full_name'] == 'Sean Goggins'
        assert result['gl_state'] == 'active'
        assert result['gh_user_id'] is None
        assert result['gh_login'] is None
    
    def test_prevents_cross_contamination(self):
        """Test that using wrong forge type would be caught."""
        # This simulates the bug: GitLab data with "github" forge type
        gitlab_data = {
            'id': 5481034,
            'username': 'computationalmystic',
            'web_url': 'https://gitlab.com/computationalmystic'
        }
        
        # If someone mistakenly uses "github" for GitLab data
        wrong_result = map_contributor_data(gitlab_data, "github")
        
        # The data wouldn't map correctly because field names don't match
        # GitHub expects 'login' but GitLab provides 'username'
        assert 'gh_login' not in wrong_result or wrong_result.get('gh_login') is None
        
        # Correct usage
        correct_result = map_contributor_data(gitlab_data, "gitlab")
        assert correct_result['gl_id'] == 5481034
        assert correct_result['gl_username'] == 'computationalmystic'


class TestValidateContributorData:
    """Tests for validate_contributor_data function."""
    
    def test_valid_github_data(self):
        """Test that valid GitHub data passes validation."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        
        github_data = {
            'cntrb_login': 'octocat',
            'gh_user_id': 583231,
            'gh_login': 'octocat',
            'gh_url': 'https://api.github.com/users/octocat',
            'gh_avatar_url': 'https://avatars.githubusercontent.com/u/583231',
            'gl_id': None,
            'gl_username': None,
            'gl_web_url': None,
            'gl_avatar_url': None,
            'gl_state': None,
            'gl_full_name': None
        }
        
        # Should not raise any exception
        validate_contributor_data(github_data, 'github')
    
    def test_valid_gitlab_data(self):
        """Test that valid GitLab data passes validation."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        
        gitlab_data = {
            'cntrb_login': 'computationalmystic',
            'gl_id': 5481034,
            'gl_username': 'computationalmystic',
            'gl_web_url': 'https://gitlab.com/computationalmystic',
            'gl_avatar_url': 'https://secure.gravatar.com/avatar/abc',
            'gl_state': 'active',
            'gl_full_name': 'Sean Goggins',
            'gh_user_id': None,
            'gh_login': None,
            'gh_url': None,
            'gh_avatar_url': None
        }
        
        # Should not raise any exception
        validate_contributor_data(gitlab_data, 'gitlab')
    
    def test_cross_contamination_github_in_gitlab(self):
        """Test that GitHub data in GitLab columns is caught."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        
        # This is the bug from issue #3469
        contaminated_data = {
            'cntrb_login': 'computationalmystic',
            'gh_user_id': 5481034,  # ❌ GitLab ID in GitHub column!
            'gh_login': 'computationalmystic',  # ❌ GitLab username in GitHub column!
            'gh_url': 'https://gitlab.com/user',  # ❌ GitLab URL in GitHub column!
            'gl_id': None,
            'gl_username': None
        }
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_data(contaminated_data, 'gitlab')
        
        error_msg = str(exc_info.value)
        assert 'gh_user_id' in error_msg
        assert 'gh_login' in error_msg
        assert 'gh_url' in error_msg
        assert 'cross-contamination' in error_msg.lower()
        assert '#3469' in error_msg
    
    def test_cross_contamination_gitlab_in_github(self):
        """Test that GitLab data in GitHub columns is caught."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        
        contaminated_data = {
            'cntrb_login': 'octocat',
            'gl_id': 583231,  # ❌ GitHub ID in GitLab column!
            'gl_username': 'octocat',  # ❌ GitHub username in GitLab column!
            'gh_user_id': None,
            'gh_login': None
        }
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_data(contaminated_data, 'github')
        
        error_msg = str(exc_info.value)
        assert 'gl_id' in error_msg
        assert 'gl_username' in error_msg
    
    def test_missing_critical_github_fields_warns(self, caplog):
        """Test that missing critical GitHub fields generates warnings."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        import logging
        
        incomplete_data = {
            'cntrb_login': 'octocat',
            'gh_url': 'https://api.github.com/users/octocat',  # Has URL but no ID/login
            'gh_user_id': None,
            'gh_login': None,
            'gl_id': None,
            'gl_username': None
        }
        
        logger = logging.getLogger('test_logger')
        with caplog.at_level(logging.WARNING):
            validate_contributor_data(incomplete_data, 'github', logger)
        
        # Should have warning about missing critical fields
        assert any('gh_user_id' in record.message for record in caplog.records)
        assert any('gh_login' in record.message for record in caplog.records)
    
    def test_missing_critical_gitlab_fields_warns(self, caplog):
        """Test that missing critical GitLab fields generates warnings."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        import logging
        
        incomplete_data = {
            'cntrb_login': 'user',
            'gl_web_url': 'https://gitlab.com/user',  # Has URL but no ID/username
            'gl_id': None,
            'gl_username': None,
            'gh_user_id': None,
            'gh_login': None
        }
        
        logger = logging.getLogger('test_logger')
        with caplog.at_level(logging.WARNING):
            validate_contributor_data(incomplete_data, 'gitlab', logger)
        
        # Should have warning about missing critical fields
        assert any('gl_id' in record.message for record in caplog.records)
        assert any('gl_username' in record.message for record in caplog.records)
    
    def test_unsupported_forge_type(self):
        """Test that unsupported forge type raises UnsupportedForgeError."""
        from augur.tasks.util.contributor_utils import validate_contributor_data, UnsupportedForgeError
        
        data = {'cntrb_login': 'user'}
        
        with pytest.raises(UnsupportedForgeError) as exc_info:
            validate_contributor_data(data, 'bitbucket')
        
        assert 'bitbucket' in str(exc_info.value).lower()
    
    def test_missing_cntrb_login_warns(self, caplog):
        """Test that missing cntrb_login generates warning."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        import logging
        
        data = {
            'gh_user_id': 123,
            'gh_login': 'octocat',
            'gl_id': None
            # cntrb_login missing!
        }
        
        logger = logging.getLogger('test_logger')
        with caplog.at_level(logging.WARNING):
            validate_contributor_data(data, 'github', logger)
        
        assert any('cntrb_login' in record.message for record in caplog.records)
    
    def test_empty_cntrb_login_warns(self, caplog):
        """Test that empty cntrb_login generates warning."""
        from augur.tasks.util.contributor_utils import validate_contributor_data
        import logging
        
        data = {
            'cntrb_login': '',  # Empty string
            'gh_user_id': 123,
            'gh_login': 'octocat',
            'gl_id': None
        }
        
        logger = logging.getLogger('test_logger')
        with caplog.at_level(logging.WARNING):
            validate_contributor_data(data, 'github', logger)
        
        assert any('cntrb_login' in record.message and 'empty' in record.message for record in caplog.records)


class TestValidateContributorBatch:
    """Tests for validate_contributor_batch function."""
    
    def test_valid_batch(self):
        """Test that a batch of valid contributors passes."""
        from augur.tasks.util.contributor_utils import validate_contributor_batch
        
        contributors = [
            {
                'cntrb_login': 'user1',
                'gl_id': 1,
                'gl_username': 'user1',
                'gh_user_id': None,
                'gh_login': None
            },
            {
                'cntrb_login': 'user2',
                'gl_id': 2,
                'gl_username': 'user2',
                'gh_user_id': None,
                'gh_login': None
            }
        ]
        
        # Should not raise
        validate_contributor_batch(contributors, 'gitlab')
    
    def test_batch_with_failures(self):
        """Test that batch validation reports all failures."""
        from augur.tasks.util.contributor_utils import validate_contributor_batch
        
        contributors = [
            {
                'cntrb_login': 'user1',
                'gl_id': 1,
                'gl_username': 'user1',
                'gh_user_id': None  # ✅ Valid
            },
            {
                'cntrb_login': 'user2',
                'gh_user_id': 2,  # ❌ Wrong column
                'gl_id': None
            },
            {
                'cntrb_login': 'user3',
                'gh_login': 'user3',  # ❌ Wrong column
                'gl_id': None
            }
        ]
        
        with pytest.raises(ValueError) as exc_info:
            validate_contributor_batch(contributors, 'gitlab')
        
        error_msg = str(exc_info.value)
        assert 'Contributor #1' in error_msg
        assert 'Contributor #2' in error_msg
        assert '2 out of 3' in error_msg
    
    def test_empty_batch(self):
        """Test that empty batch passes validation."""
        from augur.tasks.util.contributor_utils import validate_contributor_batch
        
        # Should not raise
        validate_contributor_batch([], 'github')
