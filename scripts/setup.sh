#!/bin/bash

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "uv is not installed. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Create a new virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating new virtual environment..."
    uv venv
fi

# Activate the virtual environment
source .venv/bin/activate

# Install dependencies using uv
echo "Installing dependencies..."
uv pip install -e .

# Install development dependencies
echo "Installing development dependencies..."
uv pip install black isort flake8 pytest pytest-cov

echo "Setup complete! Virtual environment is activated."
echo "To deactivate the virtual environment, run: deactivate" 