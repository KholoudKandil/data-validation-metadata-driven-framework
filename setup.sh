#!/bin/bash
# Fallback setup script for local installation (Windows Git Bash, macOS, Linux)

set -e  # Exit on error

echo "========================================"
echo "Metadata Framework - Local Setup"
echo "========================================"

# Check Python
echo ""
echo "Checking Python..."
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "❌ Python not found. Install from https://www.python.org/"
    exit 1
fi

PYTHON_CMD="python"
if ! command -v python &> /dev/null; then
    PYTHON_CMD="python3"
fi

echo "✓ Using: $PYTHON_CMD"
$PYTHON_CMD --version

# Create virtual environment
echo ""
echo "Creating virtual environment..."
$PYTHON_CMD -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "========================================"
echo "✓ Setup complete!"
echo "========================================"
echo ""
echo "To activate environment next time:"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    echo "  source venv/Scripts/activate"
else
    echo "  source venv/bin/activate"
fi
echo ""
echo "To run the pipeline:"
echo "  python src/main.py config/metadata.yaml"
echo ""
echo "To run tests:"
echo "  pytest tests/ -v"
echo ""
