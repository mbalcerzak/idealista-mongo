#!/bin/bash
# Quick script to export MongoDB data with your virtual environment

VENV_PATH=".venv"

if [ ! -d "$VENV_PATH" ]; then
    echo "❌ Virtual environment not found at $VENV_PATH"
    exit 1
fi

# Activate virtual environment and run export
source "$VENV_PATH/bin/activate"

# Check if --combined flag is passed
if [ "$1" == "--combined" ]; then
    python3 export_mongo_data.py --combined
else
    python3 export_mongo_data.py "$@"
fi

deactivate
