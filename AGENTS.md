# Agent Instructions for SQL Ranger

This document provides instructions for Copilot agents working on the SQL Ranger project.

## Project Structure

SQL Ranger is a Python library that validates SQL queries for proper partition usage. The project structure is:

```
sql-ranger/
├── src/
│   └── sqlranger/          # Main package source code
│       ├── __init__.py     # Package initialization, exports public API
│       └── checker.py      # Core partition validation logic
├── tests/
│   └── test_checker.py     # Test suite for partition checker
├── Makefile                # Build automation and common commands
├── pyproject.toml          # Poetry configuration, dependencies, and ruff settings
├── environment.yml         # Conda environment specification (Python 3.10.19, Poetry 2.2.1)
├── poetry.lock             # Locked dependency versions
└── README.md               # Project documentation
```

**Key Components:**
- **PartitionChecker**: Main class that validates SQL queries against partitioning requirements
- **PartitionCheckStatus**: Enum defining validation result statuses (VALID, MISSING_DAY_FILTER, DAY_FILTER_WITH_FUNCTION, NO_FINITE_RANGE, EXCESSIVE_DATE_RANGE)
- **PartitionCheckResult**: Dataclass containing validation results
- **check_partition_usage()**: Convenience function for simple validation

**Dependencies:**
- Python ≥ 3.10
- sqlglot ≥ 28.0.0 (SQL parsing)
- pytest, pytest-cov, pytest-asyncio (testing)
- ruff 0.14.10 (linting)

## Make Commands

SQL Ranger uses Make for common development tasks. All commands use Conda to manage the environment.

### Environment Management

```bash
make env-create        # Create conda environment and install dependencies
                      # - Creates 'sql-ranger' conda environment
                      # - Installs Poetry and dependencies
                      # - Runs poetry lock and poetry install

make env-remove       # Remove the conda environment completely

make env-update       # Update dependencies using Poetry
                     # - Runs poetry update in the conda environment
```

**Important**: The conda environment is named `sql-ranger` (defined by CONDA_ENV_NAME variable).

### Testing

```bash
make test             # Run tests with pytest
                     # - Runs: pytest tests/ -v
                     # - Executes all tests in the tests/ directory

make test-coverage    # Run tests with coverage report
                     # - Generates HTML coverage report in htmlcov/
                     # - Note: Coverage currently set for 'trino_sql_agent' 
                     #   (likely a typo that should be 'sqlranger')
```

### Linting and Code Quality

```bash
make lint            # Run ruff linting checks (read-only)
                    # - Checks: src/sqlranger/ and tests/
                    # - Must pass before committing

make ruff-fix       # Run ruff with auto-fix
                   # - Auto-fixes code style issues where possible
                   # - Uses --unsafe-fixes flag
```

### Building and Publishing

```bash
make build          # Build the package with Poetry
                   # - Creates distribution files in dist/

make publish        # Publish to PyPI
make publish-test   # Publish to Test PyPI
```

### Cleanup

```bash
make clean          # Remove generated files
                   # - Removes: dist/, build/, .egg-info, htmlcov/,
                   #   .pytest_cache/, __pycache__, .coverage
```

## Pre-commit Checklist

Before committing or creating a pull request, ensure:

1. **Linting Passes**
   ```bash
   make lint
   ```
   - All ruff checks must pass
   - No code style violations
   - Fix issues with `make ruff-fix` or manually

2. **Tests Pass**
   ```bash
   make test
   ```
   - All existing tests must pass
   - Add tests for new functionality
   - Maintain or improve test coverage

3. **Code Builds Successfully**
   ```bash
   make build
   ```
   - Package builds without errors

4. **Manual Verification**
   - Test new features manually if applicable
   - Verify edge cases
   - Check that changes work as intended

5. **Documentation Updated**
   - Update README.md if public API changes
   - Add docstrings for new functions/classes
   - Update type hints

## Style Guide

SQL Ranger follows strict Python coding standards enforced by ruff.

### General Rules

- **Line Length**: 120 characters maximum
- **Indentation**: 4 spaces (no tabs)
- **Target Version**: Python 3.10+
- **Type Hints**: Use modern Python 3.10+ type hints (e.g., `list[str]` not `List[str]`)

### Ruff Configuration

The project enables these ruff rule sets:

- **E, W**: pycodestyle errors and warnings
- **F**: pyflakes (unused imports, undefined names)
- **I**: isort (import sorting)
- **D**: pydocstyle (docstring conventions)
- **N**: pep8-naming (naming conventions)
- **UP**: pyupgrade (modern Python syntax)
- **B**: flake8-bugbear (common bugs)
- **C4**: flake8-comprehensions (list/dict comprehensions)
- **T10**: flake8-debugger (no debugger statements)
- **T20**: flake8-print (no print statements in production code)
- **SIM**: flake8-simplify (simplification suggestions)
- **PIE**: flake8-pie (miscellaneous lints)
- **RET**: flake8-return (return statement issues)
- **ASYNC**: flake8-async (async/await issues)
- **RUF**: ruff-specific rules
- **PLC**: Pylint conventions

### Docstring Conventions

**Style**: Google-style docstrings (not NumPy or Sphinx)

**Format**:
```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief description of what the function does.
    
    Longer description if needed. Can span multiple lines.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Description of return value
    
    Raises:
        ValueError: When and why this is raised
    """
```

**Ignored Rules**:
- D203, D212: Docstring formatting (specific style choice)
- D107: `__init__` docstrings not required
- D205: No blank line after summary
- D401: First line doesn't need to be imperative
- D406: Section names can end with colon
- D407: Prefer "Args:" over dashed lines
- D413: No blank line at end of docstring
- D417: Not all parameters need documentation

### Import Organization

- Standard library imports first
- Third-party imports second  
- Local imports last
- Each group separated by blank line
- Sorted alphabetically within groups

Example:
```python
"""Module docstring."""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import sqlglot
from sqlglot import exp

from .other_module import SomeClass
```

### Naming Conventions

- **Classes**: PascalCase (e.g., `PartitionChecker`)
- **Functions/Methods**: snake_case (e.g., `check_partition_usage`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `MAX_DAYS`)
- **Private members**: Prefix with `_` (e.g., `_extract_tables`)
- **Module level "private" functions**: Prefix with `_`

### Type Hints

- Always use type hints for function parameters and return values
- Use modern Python 3.10+ syntax:
  - `list[str]` not `List[str]`
  - `dict[str, int]` not `Dict[str, int]`
  - `str | None` not `Optional[str]`
  - `int | str` not `Union[int, str]`

### Code Quality Guidelines

- **No print statements**: Use proper logging (enforced by T20)
- **No debugger statements**: Remove before committing (enforced by T10)
- **Simplify comprehensions**: Use ruff suggestions (enforced by C4, SIM)
- **Proper exception handling**: Catch specific exceptions, not bare `except:`
- **Return statements**: Follow flake8-return guidelines for clean returns

### Testing Standards

- Test file naming: `test_*.py`
- Test class naming: `TestClassName`
- Test method naming: `test_descriptive_name`
- Use descriptive docstrings in tests
- Follow AAA pattern: Arrange, Act, Assert
- Use pytest conventions and fixtures

### Project-Specific Conventions

1. **SQL Parsing**: Always use `sqlglot.parse_one(sql, dialect="trino")`
2. **Error Handling**: Gracefully handle parsing failures (return empty list/None)
3. **Case Sensitivity**: Table names are case-insensitive (normalize to lowercase)
4. **Validation Results**: Always return structured `PartitionCheckResult` objects
5. **Public API**: Only export through `__init__.py` `__all__` list

## Project Boundaries

### What to Modify

- Core validation logic in `src/sqlranger/checker.py`
- Tests in `tests/test_checker.py`
- Public API exports in `src/sqlranger/__init__.py`
- Documentation in README.md
- Build configuration in pyproject.toml

### What NOT to Modify

- `.github/agents/` directory (contains agent configurations)
- Poetry lock file (use `poetry update` commands instead)
- Conda environment specification (unless explicitly required)
- Git configuration files

### External Dependencies

- **Do not add dependencies lightly**: SQL Ranger is a library that should remain lightweight
- **Check for vulnerabilities**: Always validate new dependencies
- **Prefer standard library**: Use built-in Python features when possible
- **SQLglot is core**: Do not replace or remove sqlglot dependency

### Performance Considerations

- Queries can be large; optimize parsing when possible
- Validation should be fast (part of development workflow)
- Avoid unnecessary iterations over SQL AST

### Security Considerations

- Never execute SQL queries (only parse and analyze)
- Validate inputs to prevent injection-style issues in analysis
- Be cautious with user-provided SQL strings

## Common Development Workflows

### Adding a New Validation Rule

1. Add the status to `PartitionCheckStatus` enum
2. Implement validation logic in `PartitionChecker` class
3. Add helper methods if needed (prefix with `_`)
4. Add comprehensive tests in `test_checker.py`
5. Update README.md with new validation rule
6. Run `make lint` and `make test`

### Fixing a Bug

1. Write a failing test that reproduces the bug
2. Fix the bug with minimal changes
3. Verify the test passes
4. Run full test suite with `make test`
5. Run linting with `make lint`

### Updating Dependencies

1. Update version in `pyproject.toml`
2. Run `make env-update` to update lock file
3. Test thoroughly
4. Document any breaking changes

### Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG (if exists)
3. Run `make test` and `make lint`
4. Run `make build`
5. Run `make publish-test` to test on Test PyPI
6. Run `make publish` to publish to PyPI

## Getting Help

- Read the README.md for project overview
- Check existing tests for usage examples
- Review `checker.py` docstrings for API documentation
- Run `make help` for available commands
