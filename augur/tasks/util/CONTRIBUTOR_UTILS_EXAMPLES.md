# Contributor Utils - Usage Examples

This document provides examples of how to use the contributor utilities to properly handle contributor data from different forge platforms.

## Basic Usage

### Getting Column Mappings

```python
from augur.tasks.util.contributor_utils import get_contributor_column_mapping

# Get GitHub column mapping
github_mapping = get_contributor_column_mapping("github")
print(github_mapping['user_id'])  # Output: 'gh_user_id'
print(github_mapping['login'])     # Output: 'gh_login'
print(github_mapping['url'])       # Output: 'gh_url'

# Get GitLab column mapping
gitlab_mapping = get_contributor_column_mapping("gitlab")
print(gitlab_mapping['user_id'])  # Output: 'gl_id'
print(gitlab_mapping['login'])     # Output: 'gl_username'
print(gitlab_mapping['url'])       # Output: 'gl_web_url'
```

### Automatic Data Mapping

```python
from augur.tasks.util.contributor_utils import map_contributor_data

# GitHub contributor data from API
github_contributor = {
    'id': 12345,
    'login': 'octocat',
    'url': 'https://api.github.com/users/octocat',
    'avatar_url': 'https://avatars.githubusercontent.com/u/12345',
    'node_id': 'MDQ6VXNlcjEyMzQ1',
    'type': 'User'
}

# Map to database columns
db_data = map_contributor_data(github_contributor, "github")

# Result:
# {
#     'gh_user_id': 12345,
#     'gh_login': 'octocat',
#     'gh_url': 'https://api.github.com/users/octocat',
#     'gh_avatar_url': 'https://avatars.githubusercontent.com/u/12345',
#     'gh_node_id': 'MDQ6VXNlcjEyMzQ1',
#     'gh_type': 'User',
#     'gl_id': None,
#     'gl_username': None,
#     'gl_web_url': None,
#     # ... all GitLab columns set to None
# }
```

## Integration Examples

### Example 1: Extracting GitHub Contributor Data

```python
from augur.tasks.util.contributor_utils import get_contributor_column_mapping, get_null_columns_for_other_forges
from augur.tasks.util.AugurUUID import GithubUUID

def extract_github_contributor_data(contributor, tool_source, tool_version, data_source):
    """Extract contributor data from GitHub API response."""
    
    # Get column mapping for GitHub
    mapping = get_contributor_column_mapping("github")
    null_cols = get_null_columns_for_other_forges("github")
    
    # Generate UUID
    cntrb_id = GithubUUID()
    cntrb_id["user"] = contributor["id"]
    
    # Build contributor dict using mapping
    contributor_dict = {
        "cntrb_id": cntrb_id.to_UUID(),
        "cntrb_login": contributor['login'],
        "cntrb_email": contributor.get('email'),
        "cntrb_company": contributor.get('company'),
        "cntrb_location": contributor.get('location'),
        "cntrb_created_at": contributor.get('created_at'),
        "cntrb_canonical": contributor.get('email'),
        "cntrb_full_name": contributor.get('name'),
        
        # Use mapping for GitHub-specific fields
        mapping['user_id']: contributor['id'],
        mapping['login']: str(contributor['login']),
        mapping['url']: contributor['url'],
        mapping['html_url']: contributor.get('html_url'),
        mapping['node_id']: contributor.get('node_id'),
        mapping['avatar_url']: contributor.get('avatar_url'),
        mapping['gravatar_id']: contributor.get('gravatar_id'),
        mapping['type']: contributor.get('type'),
        mapping['site_admin']: contributor.get('site_admin'),
        # ... other GitHub fields
        
        "tool_source": tool_source,
        "tool_version": tool_version,
        "data_source": data_source
    }
    
    # Add null columns for GitLab
    contributor_dict.update(null_cols)
    
    return contributor_dict
```

### Example 2: Extracting GitLab Contributor Data (CORRECTED)

```python
from augur.tasks.util.contributor_utils import get_contributor_column_mapping, get_null_columns_for_other_forges
from augur.tasks.util.AugurUUID import GitlabUUID

def extract_gitlab_contributor_data(contributor, tool_source, tool_version, data_source):
    """Extract contributor data from GitLab API response."""
    
    if not contributor:
        return None
    
    # Get column mapping for GitLab
    mapping = get_contributor_column_mapping("gitlab")
    null_cols = get_null_columns_for_other_forges("gitlab")
    
    # Generate UUID
    cntrb_id = GitlabUUID()
    cntrb_id["user"] = contributor["id"]
    
    # Build contributor dict using mapping
    contributor_dict = {
        "cntrb_id": cntrb_id.to_UUID(),
        "cntrb_login": contributor['username'],
        "cntrb_email": contributor.get('email'),
        "cntrb_company": contributor.get('company'),
        "cntrb_location": contributor.get('location'),
        "cntrb_created_at": contributor.get('created_at'),
        "cntrb_canonical": contributor.get('email'),
        "cntrb_full_name": contributor.get('name'),
        
        # Use mapping for GitLab-specific fields
        mapping['user_id']: contributor['id'],               # Maps to gl_id
        mapping['username']: str(contributor['username']),   # Maps to gl_username
        mapping['web_url']: contributor.get('web_url'),      # Maps to gl_web_url
        mapping['avatar_url']: contributor.get('avatar_url'), # Maps to gl_avatar_url
        mapping['state']: contributor.get('state'),          # Maps to gl_state
        mapping['name']: contributor.get('name'),            # Maps to gl_full_name
        
        "tool_source": tool_source,
        "tool_version": tool_version,
        "data_source": data_source
    }
    
    # Add null columns for GitHub
    contributor_dict.update(null_cols)
    
    return contributor_dict
```

### Example 3: Using the Convenience Function

The simplest approach is to use `map_contributor_data`:

```python
from augur.tasks.util.contributor_utils import map_contributor_data
from augur.tasks.util.AugurUUID import GitlabUUID

def extract_gitlab_contributor_data_simple(contributor, tool_source, tool_version, data_source):
    """Simplified extraction using map_contributor_data."""
    
    if not contributor:
        return None
    
    # Generate UUID
    cntrb_id = GitlabUUID()
    cntrb_id["user"] = contributor["id"]
    
    # Map API data to database columns automatically
    mapped_data = map_contributor_data(contributor, "gitlab")
    
    # Add common fields
    result = {
        "cntrb_id": cntrb_id.to_UUID(),
        "cntrb_login": contributor['username'],
        "cntrb_email": contributor.get('email'),
        "cntrb_company": contributor.get('company'),
        "cntrb_location": contributor.get('location'),
        "cntrb_created_at": contributor.get('created_at'),
        "cntrb_canonical": contributor.get('email'),
        "cntrb_full_name": contributor.get('name'),
        "tool_source": tool_source,
        "tool_version": tool_version,
        "data_source": data_source
    }
    
    # Merge with forge-specific columns
    result.update(mapped_data)
    
    return result
```

## Validation Examples

### Checking Supported Forge Types

```python
from augur.tasks.util.contributor_utils import validate_forge_type, get_supported_forge_types

# Check if a forge is supported
if validate_forge_type("github"):
    print("GitHub is supported")

if validate_forge_type("bitbucket"):
    print("This won't print - bitbucket not supported")

# Get all supported forges
supported = get_supported_forge_types()
print(f"Supported forges: {supported}")  # ['github', 'gitlab']
```

### Error Handling

```python
from augur.tasks.util.contributor_utils import get_contributor_column_mapping, UnsupportedForgeError

try:
    mapping = get_contributor_column_mapping("bitbucket")
except UnsupportedForgeError as e:
    print(f"Error: {e}")
    # Output: Error: Unsupported forge type: 'bitbucket'. Supported forge types are: github, gitlab
    
    # Handle the error appropriately
    supported = get_supported_forge_types()
    print(f"Please use one of: {supported}")
```

## Before and After Comparison

### Before (INCORRECT - The Bug)

```python
# augur/application/db/data_parse.py - OLD VERSION
def extract_needed_gitlab_contributor_data(contributor, tool_source, tool_version, data_source):
    contributor = {
        "cntrb_id": cntrb_id.to_UUID(),
        "cntrb_login": contributor['username'],
        
        # ❌ WRONG: GitLab data in GitHub columns
        "gh_user_id": contributor['id'],
        "gh_login": str(contributor['username']),
        "gh_url": contributor['web_url'],
        "gh_avatar_url": contributor['avatar_url'],
        # ... causes cross-contamination!
    }
```

### After (CORRECT)

```python
# Using contributor_utils
from augur.tasks.util.contributor_utils import get_contributor_column_mapping, get_null_columns_for_other_forges

def extract_needed_gitlab_contributor_data(contributor, tool_source, tool_version, data_source):
    mapping = get_contributor_column_mapping("gitlab")
    null_cols = get_null_columns_for_other_forges("gitlab")
    
    contributor = {
        "cntrb_id": cntrb_id.to_UUID(),
        "cntrb_login": contributor['username'],
        
        # ✅ CORRECT: GitLab data in GitLab columns
        mapping['user_id']: contributor['id'],           # gl_id
        mapping['username']: str(contributor['username']), # gl_username
        mapping['web_url']: contributor['web_url'],       # gl_web_url
        mapping['avatar_url']: contributor['avatar_url'], # gl_avatar_url
        mapping['state']: contributor.get('state'),       # gl_state
        mapping['name']: contributor.get('name'),         # gl_full_name
    }
    
    # Explicitly set GitHub columns to None
    contributor.update(null_cols)
```

## Testing

Run the test suite to verify functionality:

```bash
pytest tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py -v
```

Expected output:
```
tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py::TestGetContributorColumnMapping::test_github_mapping PASSED
tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py::TestGetContributorColumnMapping::test_gitlab_mapping PASSED
tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py::TestGetContributorColumnMapping::test_case_insensitive PASSED
...
```

## Additional Notes

1. **Case Insensitivity**: All forge type comparisons are case-insensitive
2. **Whitespace Handling**: Leading/trailing whitespace is automatically trimmed
3. **Extensibility**: New forge types can be added by extending the mapping dictionaries
4. **Type Safety**: The module uses type hints for better IDE support and error catching
5. **Error Messages**: Clear error messages indicate which forge types are supported
