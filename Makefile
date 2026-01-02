CONDA_ENV_NAME=sql-ranger

PYTHON_FILES=\
	src/sqlranger/ \
	tests/ \

.PHONY: help env-init env-create env-remove env-update test test-coverage lint ruff-fix clean build publish

help:
	@echo "Available targets:"
	@echo "  make env-create        - Create conda environment and install dependencies"
	@echo "  make env-remove        - Remove conda environment"
	@echo "  make env-update        - Update conda environment using Poetry"
	@echo "  make test              - Run tests"
	@echo "  make test-coverage     - Run tests with coverage report"
	@echo "  make lint              - Run code linting (if configured)"
	@echo "  make clean             - Clean up generated files"

env-init:
	@echo "Creating conda environment..."
	@which conda > /dev/null || (echo "Conda not found. Install Miniconda or Miniforge first." && exit 1)
	@conda env create -f environment.yml
	@echo "✓ Conda environment created: $(CONDA_ENV_NAME)"

env-create: env-init
	@echo "Installing dependencies with Poetry..."
	@PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) poetry lock
	@PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring conda run --name $(CONDA_ENV_NAME) --no-capture-output poetry install
	@echo "✓ Dependencies installed"
	@echo "Don't forget to create a .env file from .env.example"

env-remove:
	@echo "Removing conda environment..."
	@conda env remove --yes --name $(CONDA_ENV_NAME)
	@echo "✓ Conda environment removed: $(CONDA_ENV_NAME)"

env-update:
	@echo "Updating conda environment using Poetry..."
	@PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) poetry update
	@echo "✓ Conda environment was updated: $(CONDA_ENV_NAME)"

test:
	@echo "Running tests..."
	@conda run --name $(CONDA_ENV_NAME) --no-capture-output pytest tests/ -v

test-coverage:
	@echo "Running tests with coverage..."
	@conda run --name $(CONDA_ENV_NAME) --no-capture-output pytest tests/ --cov=sqlranger --cov-report=term-missing --cov-report=html
	@echo "✓ Coverage report generated in htmlcov/index.html"

lint:
	@echo "Run ruff checks..."
	@conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) ruff check $(PYTHON_FILES)
	@echo "✓ Ruff checks passed"

ruff-fix:
	@echo "Run ruff checks with auto-fix..."
	@conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) ruff check --fix --unsafe-fixes $(PYTHON_FILES)
	@echo "✓ Ruff auto-fix completed"

clean:
	@echo "Cleaning up generated files..."
	@rm -rf dist/
	@rm -rf build/
	@rm -rf src/sql_ranger.egg-info/
	@rm -rf htmlcov/
	@rm -rf .pytest_cache/
	@rm -rf __pycache__/
	@rm -rf tests/__pycache__/
	@rm -rf .coverage
	@echo "✓ Cleanup complete"

build:
	@echo "Building the package..."
	@conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) poetry build
	@echo "✓ Package built in the dist/ directory"

publish:
	@echo "Publishing the package to PyPI..."
	@conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) poetry publish
	@echo "✓ Package published to PyPI"

publish-test:
	@echo "Publishing the package to Test PyPI..."
	@conda run --no-capture-output --live-stream --name $(CONDA_ENV_NAME) poetry publish -r testpypi
	@echo "✓ Package published to Test PyPI"

.env:
	@echo "Creating .env file from .env.example..."
	@test -f .env.example && cp .env.example .env || (echo "Error: .env.example not found" && exit 1)
	@echo "✓ Created .env file. Please edit it with your Trino credentials."
