# Agent Instructions for SQL Ranger

This document provides instructions for Copilot agents working on the SQL Ranger project.

## Project Structure

SQL Ranger is a Python library that validates SQL queries for proper partition usage. The project structure is:

```
sql-ranger/
├── src/
│   └── sqlranger/          # Main package source code
│       ├── init.py         # Package initialization, exports public API
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
- **PartitionViolation**: Dataclass containing validation results
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
make env-create        # Create 'sql-ranger' conda environment and install dependencies
make env-remove        # Remove the conda environment completely
make env-update        # Update dependencies using Poetry
```

**Important**: The conda environment is named `sql-ranger` (defined by CONDA_ENV_NAME variable).

### Testing

```bash
make test             # Run tests with pytest
make test-coverage    # Run tests with coverage report. Generates HTML coverage report in htmlcov/
```

### Linting and Code Quality

```bash
make lint            # Run ruff linting checks (read-only)
make ruff-fix        # Run ruff with auto-fix
```

### Building and Publishing

```bash
make build          # Build the package with Poetry. Creates distribution files in dist/
make publish        # Publish to PyPI
make publish-test   # Publish to Test PyPI
```

### Cleanup

```bash
make clean          # Remove generated files
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

### Code Quality Guidelines

- **No print statements**: Use proper logging (enforced by T20)
- **No debugger statements**: Remove before committing (enforced by T10)
- **Simplify comprehensions**: Use ruff suggestions (enforced by C4, SIM)
- **Proper exception handling**: Catch specific exceptions, not bare `except:`
- **Return statements**: Follow flake8-return guidelines for clean returns

### Testing Standards

- Use pytest conventions and fixtures

### Project-Specific Conventions

1. **Error Handling**: Gracefully handle parsing failures (return empty list/None)
2. **Case Sensitivity**: Table names are case-insensitive (normalize to lowercase)
3. **Public API**: Only export through `__init__.py` `__all__` list

## Project Boundaries

### What to Modify

- Core validation logic in `src/sqlranger/checker.py`
- Tests in `tests/test_checker.py`
- Public API exports in `src/sqlranger/__init__.py`
- Documentation in README.md
- Build configuration in pyproject.toml

### What NOT to Modify

- Poetry lock file (use `poetry update` commands instead)
- Conda environment specification (unless explicitly required)
- Git configuration files

### External Dependencies

- **Do not add dependencies lightly**: SQL Ranger is a library that should remain lightweight
- **Prefer standard library**: Use built-in Python features when possible
