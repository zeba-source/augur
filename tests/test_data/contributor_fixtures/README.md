# Contributor Test Fixtures

This directory contains realistic test data for verifying the PR #3469 fix (GitLab contributor cross-contamination).

## Files Overview

### API Response Fixtures

| File | Description | Use Case |
|------|-------------|----------|
| `github_contributor_response.json` | Mock GitHub API user response | Testing GitHub data extraction |
| `gitlab_contributor_response.json` | Mock GitLab API user response | Testing GitLab data extraction |

### Database Record Fixtures

| File | Description | Use Case |
|------|-------------|----------|
| `contaminated_db_record.json` | **BEFORE FIX**: GitLab data in `gh_*` columns | Demonstrates the bug from issue #3469 |
| `correct_db_record_github.json` | **AFTER FIX**: Proper GitHub record | Expected output for GitHub contributors |
| `correct_db_record_gitlab.json` | **AFTER FIX**: Proper GitLab record | Expected output for GitLab contributors |

### Edge Case Fixtures

| File | Description | Use Case |
|------|-------------|----------|
| `edge_cases.json` | 9 comprehensive edge cases | Testing boundary conditions and special scenarios |

---

## Usage Examples

### 1. Unit Testing with Mock API Responses

```python
import json
from pathlib import Path

# Load GitHub API mock response
fixtures_dir = Path(__file__).parent / "test_data" / "contributor_fixtures"
with open(fixtures_dir / "github_contributor_response.json") as f:
    github_data = json.load(f)
    api_response = github_data["data"]

# Test GitHub extraction
from augur.application.db.data_parse import extract_needed_contributor_data
result = extract_needed_contributor_data(
    api_response, "GitHub API", "1.0", "api"
)

# Verify correct columns used
assert result["gh_user_id"] == 1234567
assert result["gh_login"] == "sgoggins"
assert result["gl_id"] is None
```

### 2. Validation Testing with Contaminated Data

```python
import json

# Load contaminated record (demonstrates bug)
with open(fixtures_dir / "contaminated_db_record.json") as f:
    contaminated = json.load(f)
    record = contaminated["record"]

# Verify validation catches cross-contamination
from augur.tasks.util.contributor_utils import validate_contributor_data

try:
    validate_contributor_data(record, "gitlab")
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "Cross-contamination detected" in str(e)
    assert "#3469" in str(e)  # References the issue
```

### 3. Integration Testing with Correct Records

```python
# Load correct GitLab record
with open(fixtures_dir / "correct_db_record_gitlab.json") as f:
    correct_gitlab = json.load(f)
    record = correct_gitlab["record"]

# Verify validation passes
validate_contributor_data(record, "gitlab")  # Should not raise

# Verify correct column usage
assert record["gl_id"] == 5481034
assert record["gl_username"] == "computationalmystic"
assert record["gh_user_id"] is None
assert record["gh_login"] is None
```

### 4. Edge Case Testing

```python
# Load edge cases
with open(fixtures_dir / "edge_cases.json") as f:
    edge_cases = json.load(f)

# Test same username on both platforms
same_username_case = next(
    case for case in edge_cases["test_cases"]
    if case["case_name"] == "same_username_both_platforms"
)

github_record = same_username_case["github_record"]
gitlab_record = same_username_case["gitlab_record"]

# Both should have same cntrb_login but different cntrb_id
assert github_record["cntrb_login"] == gitlab_record["cntrb_login"]
assert github_record["cntrb_id"] != gitlab_record["cntrb_id"]

# Verify no cross-contamination
assert github_record["gh_user_id"] is not None
assert github_record["gl_id"] is None
assert gitlab_record["gl_id"] is not None
assert gitlab_record["gh_user_id"] is None
```

### 5. Database Migration Testing

```python
# Simulate migration: contaminated → correct
with open(fixtures_dir / "contaminated_db_record.json") as f:
    before = json.load(f)["record"]

with open(fixtures_dir / "correct_db_record_gitlab.json") as f:
    after = json.load(f)["record"]

# Verify migration moves data correctly
assert before["gh_user_id"] == after["gl_id"]  # 5481034
assert before["gh_login"] == after["gl_username"]  # "computationalmystic"
assert before["gh_url"].replace("https://gitlab.com/", "https://gitlab.com/") == \
       after["gl_web_url"]
assert after["gh_user_id"] is None
assert after["gh_login"] is None
```

---

## Data Structure Reference

### GitHub Contributor Columns (18 columns)

```python
GH_COLUMNS = [
    "gh_user_id",           # Integer: GitHub user ID
    "gh_login",             # String: GitHub username
    "gh_url",               # String: API URL
    "gh_html_url",          # String: Web URL
    "gh_node_id",           # String: GraphQL node ID
    "gh_avatar_url",        # String: Avatar image URL
    "gh_gravatar_id",       # String: Gravatar hash (usually empty)
    "gh_followers_url",     # String: API endpoint
    "gh_following_url",     # String: API endpoint
    "gh_gists_url",         # String: API endpoint
    "gh_starred_url",       # String: API endpoint
    "gh_subscriptions_url", # String: API endpoint
    "gh_organizations_url", # String: API endpoint
    "gh_repos_url",         # String: API endpoint
    "gh_events_url",        # String: API endpoint
    "gh_received_events_url", # String: API endpoint
    "gh_type",              # String: "User" or "Bot"
    "gh_site_admin"         # Boolean: GitHub staff flag
]
```

### GitLab Contributor Columns (6 columns)

```python
GL_COLUMNS = [
    "gl_id",           # Integer: GitLab user ID
    "gl_username",     # String: GitLab username
    "gl_web_url",      # String: Web URL
    "gl_avatar_url",   # String: Avatar image URL
    "gl_state",        # String: "active", "blocked", "banned", "ldap_blocked"
    "gl_full_name"     # String: Full display name
]
```

### Common Columns (Both Platforms)

```python
COMMON_COLUMNS = [
    "cntrb_id",              # UUID: Universal contributor ID
    "cntrb_login",           # String: Username (universal field)
    "cntrb_created_at",      # Timestamp: Account creation date
    "cntrb_email",           # String: Email address
    "cntrb_company",         # String: Company/organization
    "cntrb_location",        # String: Location
    "cntrb_type",            # String: "User" or "Bot"
    "cntrb_canonical",       # String: Canonical identifier
    "cntrb_full_name",       # String: Full display name
    "cntrb_last_used",       # Timestamp: Last collection date
    "tool_source",           # String: Collection tool name
    "tool_version",          # String: Tool version
    "data_source",           # String: API source
    "data_collection_date"   # Timestamp: When collected
]
```

---

## Validation Rules

### For GitHub Contributors (forge_type="github")

✅ **MUST have:**
- `gh_user_id` populated
- `gh_login` populated
- All other `gh_*` columns populated or explicitly NULL

✅ **MUST NOT have:**
- Any `gl_*` columns populated (all should be NULL)

✅ **MUST satisfy:**
- `GH-UNIQUE-C` constraint: `gh_login` is unique among GitHub users
- `cntrb_login` equals `gh_login`

### For GitLab Contributors (forge_type="gitlab")

✅ **MUST have:**
- `gl_id` populated
- `gl_username` populated
- All other `gl_*` columns populated or explicitly NULL

✅ **MUST NOT have:**
- Any `gh_*` columns populated (all should be NULL)

✅ **MUST satisfy:**
- `GL-UNIQUE-B` constraint: `gl_id` is unique among GitLab users
- `GL-UNIQUE-C` constraint: `gl_username` is unique among GitLab users
- `cntrb_login` equals `gl_username`

---

## Test Scenarios Covered

### ✅ Basic Scenarios
1. **GitHub contributor** - All `gh_*` populated, `gl_*` NULL
2. **GitLab contributor** - All `gl_*` populated, `gh_*` NULL
3. **Contaminated data** - GitLab data in `gh_*` columns (bug)

### ✅ Edge Cases (9 scenarios in `edge_cases.json`)
1. **Same username on both platforms** - Valid, separate records
2. **Special characters in username** - Hyphens, underscores, numbers
3. **NULL optional fields** - Minimal profile with missing data
4. **Bot account** - Type="Bot", `[bot]` suffix
5. **Deleted/blocked user** - State="blocked", Ghost User
6. **Username changed** - Same `cntrb_id`, updated username
7. **Missing created_at date** - NULL for legacy accounts
8. **Self-hosted GitLab** - Custom domain URLs

### ✅ Validation Scenarios
- Cross-contamination detection
- Constraint violation prevention
- NULL safety with `.get()` method
- Platform-specific column mapping

---

## Maintenance

When adding new test fixtures:

1. **Follow naming convention**: `{description}_{platform}.json`
2. **Include metadata**: `description`, `notes`, `validation` sections
3. **Use realistic data**: Based on actual API responses
4. **Document edge cases**: Explain why the case matters
5. **Update this README**: Add to the usage examples

---

## Related Files

- **Code Under Test**: `augur/application/db/data_parse.py`
- **Utilities**: `augur/tasks/util/contributor_utils.py`
- **Unit Tests**: `tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py`
- **Integration Tests**: `tests/test_tasks/test_gitlab_contributor_handling.py`
- **Database Tests**: `tests/test_integration/test_gitlab_github_separation.py`

---

## References

- **Issue**: [#3469 - GitLab contributor cross-contamination](https://github.com/chaoss/augur/issues/3469)
- **GitHub API Docs**: https://docs.github.com/en/rest/users
- **GitLab API Docs**: https://docs.gitlab.com/ee/api/users.html
- **PR Description**: See `PR_DESCRIPTION.md`

---

**Last Updated**: December 28, 2025  
**PR**: #3469  
**Author**: Augur Development Team
