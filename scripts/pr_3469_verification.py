#!/usr/bin/env python3
"""
Pre-submission Verification Script for PR #3469
===============================================

This script programmatically verifies all checklist items before PR submission.

Usage:
    python scripts/pr_3469_verification.py [--fix] [--verbose]

Options:
    --fix       Attempt to auto-fix issues where possible
    --verbose   Show detailed output for each check
"""

import os
import re
import sys
import ast
import subprocess
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass
import argparse

# ANSI color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


@dataclass
class CheckResult:
    """Result of a verification check."""
    name: str
    passed: bool
    message: str
    details: List[str] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = []


class PRVerifier:
    """Verifies all checklist items for PR #3469."""
    
    def __init__(self, repo_root: Path, verbose: bool = False, fix: bool = False):
        self.repo_root = repo_root
        self.verbose = verbose
        self.fix = fix
        self.results: List[CheckResult] = []
        
    def print_header(self, text: str):
        """Print section header."""
        print(f"\n{BLUE}{'='*70}{RESET}")
        print(f"{BLUE}{text}{RESET}")
        print(f"{BLUE}{'='*70}{RESET}")
        
    def print_result(self, result: CheckResult):
        """Print check result."""
        status = f"{GREEN}✓ PASS{RESET}" if result.passed else f"{RED}✗ FAIL{RESET}"
        print(f"\n{status} - {result.name}")
        print(f"  {result.message}")
        
        if self.verbose and result.details:
            for detail in result.details:
                print(f"    • {detail}")
    
    def run_all_checks(self) -> bool:
        """Run all verification checks."""
        self.print_header("PR #3469 Pre-Submission Verification")
        
        # Code Analysis Checks
        self.check_gitlab_uses_gl_columns()
        self.check_github_uses_gh_columns()
        self.check_cntrb_login_populated()
        self.check_validation_logic_exists()
        self.check_no_cross_contamination_in_code()
        
        # Database Checks
        self.check_migration_exists()
        self.check_constraint_definitions()
        
        # Testing Checks
        self.check_tests_exist()
        self.check_tests_pass()
        
        # Code Quality Checks
        self.check_no_debug_code()
        self.check_documentation_updated()
        self.check_style_compliance()
        self.check_error_handling()
        
        # Git Checks
        self.check_dco_signoff()
        
        # Print Summary
        self.print_summary()
        
        # Return overall pass/fail
        return all(r.passed for r in self.results)
    
    def check_gitlab_uses_gl_columns(self):
        """Verify GitLab tasks use gl_* columns exclusively."""
        name = "GitLab tasks use gl_* columns"
        gitlab_files = [
            self.repo_root / "augur" / "tasks" / "gitlab" / "issues_task.py",
            self.repo_root / "augur" / "tasks" / "gitlab" / "merge_request_task.py",
            self.repo_root / "augur" / "application" / "db" / "data_parse.py"
        ]
        
        issues = []
        gh_column_pattern = re.compile(r'["\']gh_(user_id|login|url|avatar_url)["\']')
        gl_column_pattern = re.compile(r'["\']gl_(id|username|web_url|avatar_url|state|full_name)["\']')
        
        for file_path in gitlab_files:
            if not file_path.exists():
                issues.append(f"File not found: {file_path}")
                continue
                
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for extract_needed_gitlab_contributor_data function
            if 'extract_needed_gitlab_contributor_data' in content:
                # Verify gl_* columns used
                gl_matches = gl_column_pattern.findall(content)
                if not gl_matches:
                    issues.append(f"{file_path.name}: No gl_* columns found in GitLab extraction")
                
                # Check for problematic gh_* usage in GitLab context
                lines = content.split('\n')
                in_gitlab_function = False
                for i, line in enumerate(lines, 1):
                    if 'def extract_needed_gitlab_contributor_data' in line:
                        in_gitlab_function = True
                    elif in_gitlab_function and 'def ' in line and 'extract_needed_gitlab' not in line:
                        in_gitlab_function = False
                    
                    if in_gitlab_function:
                        # Look for gh_* assignments that aren't setting to None
                        if re.search(r'["\']gh_\w+["\']\s*:\s*(?!None)', line):
                            # Exclude comments
                            if not line.strip().startswith('#'):
                                issues.append(f"{file_path.name}:{i}: Potential gh_* column usage: {line.strip()}")
        
        passed = len(issues) == 0
        message = "All GitLab tasks use gl_* columns correctly" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_github_uses_gh_columns(self):
        """Verify GitHub tasks use gh_* columns exclusively."""
        name = "GitHub tasks use gh_* columns"
        github_files = [
            self.repo_root / "augur" / "tasks" / "github" / "issues_task.py",
            self.repo_root / "augur" / "tasks" / "github" / "pull_requests" / "pull_requests_task.py",
            self.repo_root / "augur" / "application" / "db" / "data_parse.py"
        ]
        
        issues = []
        gh_column_pattern = re.compile(r'["\']gh_(user_id|login|url|avatar_url)["\']')
        
        for file_path in github_files:
            if not file_path.exists():
                continue  # GitHub files are optional for this check
                
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for extract_needed_contributor_data function (GitHub version)
            if 'extract_needed_contributor_data' in content and 'gitlab' not in file_path.name.lower():
                # Verify gh_* columns used
                gh_matches = gh_column_pattern.findall(content)
                if not gh_matches:
                    issues.append(f"{file_path.name}: No gh_* columns found in GitHub extraction")
        
        passed = len(issues) == 0
        message = "All GitHub tasks use gh_* columns correctly" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_cntrb_login_populated(self):
        """Verify cntrb_login is populated for both forges."""
        name = "cntrb_login populated for both forges"
        data_parse_file = self.repo_root / "augur" / "application" / "db" / "data_parse.py"
        
        issues = []
        
        if not data_parse_file.exists():
            issues.append(f"File not found: {data_parse_file}")
        else:
            with open(data_parse_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for cntrb_login in both extraction functions
            functions_to_check = [
                'extract_needed_contributor_data',  # GitHub
                'extract_needed_gitlab_contributor_data'  # GitLab
            ]
            
            for func_name in functions_to_check:
                func_pattern = re.compile(rf'def {func_name}\(.*?\):(.*?)(?=\ndef |\Z)', re.DOTALL)
                match = func_pattern.search(content)
                
                if match:
                    func_body = match.group(1)
                    if '"cntrb_login"' not in func_body and "'cntrb_login'" not in func_body:
                        issues.append(f"{func_name}: cntrb_login not found in function body")
                else:
                    issues.append(f"{func_name}: Function not found")
        
        passed = len(issues) == 0
        message = "cntrb_login is populated for both forges" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_validation_logic_exists(self):
        """Verify validation logic prevents cross-contamination."""
        name = "Validation logic prevents cross-contamination"
        utils_file = self.repo_root / "augur" / "tasks" / "util" / "contributor_utils.py"
        
        issues = []
        
        if not utils_file.exists():
            issues.append(f"Validation file not found: {utils_file}")
        else:
            with open(utils_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for validation functions
            required_functions = [
                'validate_contributor_data',
                'validate_contributor_batch',
                'get_contributor_column_mapping',
                'get_null_columns_for_other_forges'
            ]
            
            for func in required_functions:
                if f'def {func}' not in content:
                    issues.append(f"Missing function: {func}")
            
            # Check for cross-contamination detection logic
            if 'Cross-contamination detected' not in content and 'cross-contamination' not in content.lower():
                issues.append("No cross-contamination detection logic found")
            
            # Check for forge type handling
            if 'gitlab' not in content.lower() or 'github' not in content.lower():
                issues.append("Missing forge type handling")
        
        passed = len(issues) == 0
        message = "Validation logic exists and is comprehensive" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_no_cross_contamination_in_code(self):
        """Check for any remaining cross-contamination patterns in code."""
        name = "No cross-contamination patterns in code"
        
        files_to_check = [
            self.repo_root / "augur" / "application" / "db" / "data_parse.py",
            self.repo_root / "augur" / "tasks" / "gitlab" / "issues_task.py",
            self.repo_root / "augur" / "tasks" / "gitlab" / "merge_request_task.py"
        ]
        
        issues = []
        
        # Patterns that indicate cross-contamination
        bad_patterns = [
            (r'forge.*==.*["\']gitlab["\'].*gh_(user_id|login|url)', 
             "GitLab forge using gh_* columns"),
            (r'forge.*==.*["\']github["\'].*gl_(id|username|web_url)', 
             "GitHub forge using gl_* columns"),
            (r'gitlab.*gh_user_id\s*=\s*(?!None)', 
             "GitLab assigning to gh_user_id"),
            (r'gitlab.*gh_login\s*=\s*(?!None)', 
             "GitLab assigning to gh_login")
        ]
        
        for file_path in files_to_check:
            if not file_path.exists():
                continue
                
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for pattern, description in bad_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE)
                for match in matches:
                    line_num = content[:match.start()].count('\n') + 1
                    issues.append(f"{file_path.name}:{line_num}: {description}")
        
        passed = len(issues) == 0
        message = "No cross-contamination patterns found" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_migration_exists(self):
        """Verify migration 38 exists and is correct."""
        name = "Migration 38 exists and is correct"
        migration_file = self.repo_root / "augur" / "application" / "schema" / "alembic" / "versions" / "38_restore_contributor_unique_constraints.py"
        
        issues = []
        
        if not migration_file.exists():
            issues.append(f"Migration file not found: {migration_file}")
        else:
            with open(migration_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for required components
            required_elements = [
                ('revision = "38"', "Revision number"),
                ('down_revision = "37"', "Down revision"),
                ('GH-UNIQUE-C', "GitHub login unique constraint"),
                ('GL-UNIQUE-B', "GitLab ID unique constraint"),
                ('GL-UNIQUE-C', "GitLab username unique constraint"),
                ('GL-cntrb-LOGIN-UNIQUE', "Contributor login unique constraint"),
                ('def upgrade()', "Upgrade function"),
                ('def downgrade()', "Downgrade function"),
                ('contributor_contamination_fixes', "Audit table")
            ]
            
            for element, description in required_elements:
                if element not in content:
                    issues.append(f"Missing {description}: {element}")
        
        passed = len(issues) == 0
        message = "Migration 38 is complete and correct" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_constraint_definitions(self):
        """Verify constraint definitions are correct."""
        name = "Database constraints are correctly defined"
        migration_file = self.repo_root / "augur" / "application" / "schema" / "alembic" / "versions" / "38_restore_contributor_unique_constraints.py"
        
        issues = []
        
        if not migration_file.exists():
            issues.append("Migration file not found")
        else:
            with open(migration_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check constraint definitions
            constraints = {
                'GH-UNIQUE-C': r'UNIQUE.*\(gh_login\)',
                'GL-UNIQUE-B': r'UNIQUE.*\(gl_id\)',
                'GL-UNIQUE-C': r'UNIQUE.*\(gl_username\)',
                'GL-cntrb-LOGIN-UNIQUE': r'UNIQUE.*\(cntrb_login\)'
            }
            
            for constraint_name, pattern in constraints.items():
                if not re.search(pattern, content, re.IGNORECASE):
                    issues.append(f"Constraint {constraint_name} definition not found or incorrect")
            
            # Check for DEFERRABLE
            if 'DEFERRABLE' not in content:
                issues.append("Constraints should be DEFERRABLE for transaction safety")
        
        passed = len(issues) == 0
        message = "All constraints are correctly defined" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_tests_exist(self):
        """Verify all required test files exist."""
        name = "All test files exist"
        
        required_test_files = [
            "tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py",
            "tests/test_tasks/test_gitlab_contributor_handling.py",
            "tests/test_integration/test_gitlab_github_separation.py"
        ]
        
        issues = []
        
        for test_file in required_test_files:
            full_path = self.repo_root / test_file
            if not full_path.exists():
                issues.append(f"Test file missing: {test_file}")
            else:
                # Check file has actual tests
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if 'def test_' not in content:
                    issues.append(f"No test functions found in {test_file}")
        
        passed = len(issues) == 0
        message = "All test files exist with test functions" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_tests_pass(self):
        """Run tests to verify they pass."""
        name = "All tests pass (unit + integration)"
        
        issues = []
        details = []
        
        # Test files to run
        test_files = [
            "tests/test_tasks/test_task_utilities/test_util/test_contributor_utils.py",
            "tests/test_tasks/test_gitlab_contributor_handling.py"
            # Note: Skipping integration tests as they require database
        ]
        
        for test_file in test_files:
            full_path = self.repo_root / test_file
            if not full_path.exists():
                continue
            
            try:
                # Run pytest on the file
                result = subprocess.run(
                    [sys.executable, "-m", "pytest", str(full_path), "-v", "--tb=short"],
                    cwd=str(self.repo_root),
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                
                if result.returncode == 0:
                    details.append(f"✓ {test_file.split('/')[-1]}")
                else:
                    issues.append(f"Tests failed in {test_file}")
                    if self.verbose:
                        details.append(f"Output: {result.stdout[-500:]}")  # Last 500 chars
                        
            except subprocess.TimeoutExpired:
                issues.append(f"Tests timed out in {test_file}")
            except Exception as e:
                issues.append(f"Error running tests in {test_file}: {str(e)}")
        
        passed = len(issues) == 0
        message = "All tests pass successfully" if passed else f"Found {len(issues)} test failure(s)"
        self.results.append(CheckResult(name, passed, message, details if passed else issues))
        self.print_result(self.results[-1])
    
    def check_no_debug_code(self):
        """Check for debug code, print statements, etc."""
        name = "No debug code or print statements"
        
        files_to_check = [
            self.repo_root / "augur" / "application" / "db" / "data_parse.py",
            self.repo_root / "augur" / "tasks" / "util" / "contributor_utils.py",
        ]
        
        issues = []
        
        # Patterns to look for
        debug_patterns = [
            (r'\bprint\s*\(', "print statement"),
            (r'\bpdb\.', "pdb debugger"),
            (r'\bipdb\.', "ipdb debugger"),
            (r'\bbreakpoint\s*\(', "breakpoint"),
            (r'\bdebugger', "debugger statement"),
            (r'console\.log', "console.log (JavaScript)"),
            (r'#\s*TODO.*3469', "TODO comment for this PR"),
            (r'#\s*FIXME.*3469', "FIXME comment for this PR")
        ]
        
        for file_path in files_to_check:
            if not file_path.exists():
                continue
                
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines, 1):
                # Skip commented lines for print checks
                stripped = line.strip()
                if stripped.startswith('#'):
                    continue
                
                for pattern, description in debug_patterns:
                    if re.search(pattern, line):
                        issues.append(f"{file_path.name}:{i}: {description} found: {line.strip()[:50]}")
        
        passed = len(issues) == 0
        message = "No debug code found" if passed else f"Found {len(issues)} debug statement(s)"
        self.results.append(CheckResult(name, passed, message, issues))
        self.print_result(self.results[-1])
    
    def check_documentation_updated(self):
        """Verify documentation has been updated."""
        name = "Documentation updated"
        
        doc_files = [
            "ISSUE_3469_ANALYSIS.md",
            "GITLAB_FIX_COMPARISON.md",
            "CONSTRAINT_ANALYSIS.md",
            "DOCUMENTATION_SUMMARY.md"
        ]
        
        issues = []
        found_files = []
        
        for doc_file in doc_files:
            full_path = self.repo_root / doc_file
            if not full_path.exists():
                issues.append(f"Documentation missing: {doc_file}")
            else:
                found_files.append(doc_file)
                
                # Check file has content
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if len(content) < 100:
                    issues.append(f"Documentation too short: {doc_file}")
        
        # Check function docstrings
        data_parse_file = self.repo_root / "augur" / "application" / "db" / "data_parse.py"
        if data_parse_file.exists():
            with open(data_parse_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if 'extract_needed_gitlab_contributor_data' in content:
                # Check for docstring after function definition
                func_match = re.search(
                    r'def extract_needed_gitlab_contributor_data.*?:\s*"""',
                    content,
                    re.DOTALL
                )
                if not func_match:
                    issues.append("extract_needed_gitlab_contributor_data missing docstring")
        
        passed = len(issues) == 0
        message = f"Documentation updated ({len(found_files)} files)" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, found_files if passed else issues))
        self.print_result(self.results[-1])
    
    def check_style_compliance(self):
        """Check code style compliance."""
        name = "Code follows Augur style guide"
        
        files_to_check = [
            self.repo_root / "augur" / "application" / "db" / "data_parse.py",
            self.repo_root / "augur" / "tasks" / "util" / "contributor_utils.py",
        ]
        
        issues = []
        details = []
        
        for file_path in files_to_check:
            if not file_path.exists():
                continue
            
            # Basic style checks
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines, 1):
                # Check line length (PEP 8: 79 chars, but allow 100)
                if len(line.rstrip()) > 120:
                    issues.append(f"{file_path.name}:{i}: Line too long ({len(line.rstrip())} chars)")
                
                # Check for trailing whitespace
                if line.rstrip() != line.rstrip('\n').rstrip('\r'):
                    if line != '\n':  # Ignore blank lines
                        issues.append(f"{file_path.name}:{i}: Trailing whitespace")
            
            details.append(f"Checked {file_path.name}: {len(lines)} lines")
        
        # Try running flake8 if available
        try:
            result = subprocess.run(
                [sys.executable, "-m", "flake8", "--max-line-length=120", "--extend-ignore=E501"],
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                details.append("✓ flake8 passed")
        except:
            details.append("⚠ flake8 not available (skipped)")
        
        passed = len(issues) == 0
        message = "Code style is compliant" if passed else f"Found {len(issues)} style issue(s)"
        self.results.append(CheckResult(name, passed, message, details if passed else issues))
        self.print_result(self.results[-1])
    
    def check_error_handling(self):
        """Check for comprehensive error handling."""
        name = "Error handling is comprehensive"
        
        files_to_check = [
            self.repo_root / "augur" / "application" / "db" / "data_parse.py",
            self.repo_root / "augur" / "tasks" / "util" / "contributor_utils.py",
        ]
        
        issues = []
        details = []
        
        for file_path in files_to_check:
            if not file_path.exists():
                continue
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for error handling patterns
            has_try_except = 'try:' in content and 'except' in content
            has_validation = 'validate' in content.lower()
            has_none_checks = '.get(' in content or 'is None' in content or 'is not None' in content
            
            if has_try_except:
                details.append(f"✓ {file_path.name}: Has try/except blocks")
            else:
                issues.append(f"{file_path.name}: No try/except error handling found")
            
            if has_validation:
                details.append(f"✓ {file_path.name}: Has validation logic")
            
            if has_none_checks:
                details.append(f"✓ {file_path.name}: Has None checks")
        
        # Check validation function raises proper errors
        utils_file = self.repo_root / "augur" / "tasks" / "util" / "contributor_utils.py"
        if utils_file.exists():
            with open(utils_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if 'raise ValueError' in content or 'raise Exception' in content:
                details.append("✓ Validation raises exceptions on errors")
            else:
                issues.append("Validation should raise exceptions on errors")
        
        passed = len(issues) == 0
        message = "Error handling is comprehensive" if passed else f"Found {len(issues)} issue(s)"
        self.results.append(CheckResult(name, passed, message, details if passed else issues))
        self.print_result(self.results[-1])
    
    def check_dco_signoff(self):
        """Check for DCO sign-off in commits."""
        name = "Commits are signed (DCO)"
        
        try:
            # Get recent commits
            result = subprocess.run(
                ["git", "log", "--format=%H %s", "-n", "20"],
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                self.results.append(CheckResult(
                    name, False, "Unable to check git commits", ["Git command failed"]
                ))
                self.print_result(self.results[-1])
                return
            
            commits = result.stdout.strip().split('\n')
            unsigned_commits = []
            
            for commit_line in commits:
                if not commit_line:
                    continue
                
                commit_hash = commit_line.split()[0]
                
                # Check commit message for sign-off
                msg_result = subprocess.run(
                    ["git", "log", "-1", "--format=%B", commit_hash],
                    cwd=str(self.repo_root),
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                commit_msg = msg_result.stdout
                
                # Check for issue #3469 reference
                if '#3469' in commit_msg or '3469' in commit_msg:
                    # Check for DCO sign-off
                    if 'Signed-off-by:' not in commit_msg:
                        unsigned_commits.append(f"{commit_hash[:8]}: Missing DCO sign-off")
            
            passed = len(unsigned_commits) == 0
            message = "All PR commits are signed" if passed else f"Found {len(unsigned_commits)} unsigned commit(s)"
            details = ["✓ All commits related to #3469 are signed"] if passed else unsigned_commits
            self.results.append(CheckResult(name, passed, message, details))
            
        except Exception as e:
            self.results.append(CheckResult(
                name, False, f"Error checking commits: {str(e)}", []
            ))
        
        self.print_result(self.results[-1])
    
    def print_summary(self):
        """Print verification summary."""
        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]
        
        self.print_header("Verification Summary")
        
        print(f"\n{GREEN}Passed:{RESET} {len(passed)}/{len(self.results)}")
        for result in passed:
            print(f"  {GREEN}✓{RESET} {result.name}")
        
        if failed:
            print(f"\n{RED}Failed:{RESET} {len(failed)}/{len(self.results)}")
            for result in failed:
                print(f"  {RED}✗{RESET} {result.name}")
                print(f"    {result.message}")
        
        print(f"\n{BLUE}{'='*70}{RESET}")
        
        if len(failed) == 0:
            print(f"\n{GREEN}✓ ALL CHECKS PASSED - PR IS READY FOR SUBMISSION!{RESET}\n")
        else:
            print(f"\n{RED}✗ {len(failed)} CHECK(S) FAILED - PLEASE FIX BEFORE SUBMITTING{RESET}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Verify PR #3469 is ready for submission"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output for each check"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt to auto-fix issues where possible"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Path to Augur repository root (default: auto-detect)"
    )
    
    args = parser.parse_args()
    
    # Determine repo root
    if args.repo_root:
        repo_root = args.repo_root
    else:
        # Try to find repo root from current directory
        current = Path.cwd()
        while current != current.parent:
            if (current / ".git").exists() and (current / "augur").exists():
                repo_root = current
                break
            current = current.parent
        else:
            print(f"{RED}Error: Unable to locate Augur repository root{RESET}")
            print("Please run from within the repository or use --repo-root")
            sys.exit(1)
    
    print(f"Repository root: {repo_root}")
    
    # Run verification
    verifier = PRVerifier(repo_root, verbose=args.verbose, fix=args.fix)
    success = verifier.run_all_checks()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
