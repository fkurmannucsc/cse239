#!/bin/bash

# Automates the process of downloading the enwik9 dataset preparing it, and running the wordcount.py benchmark.

# Exit immediately if any command fails.
set -e

# Config.
DATASET_URL="https://mattmahoney.net/dc/enwik9.zip"
ZIP_FILE="enwik9.zip"
INPUT_DIR="txt"
SCRIPT_NAME="wordcount.py"

# Main Script.
echo "Wordcount script starting."

# Download the dataset if it doesn't exist.
if [ ! -f "$ZIP_FILE" ]; then
    echo "Downloading enwik9 dataset from $DATASET_URL..."
    curl -L -o "$ZIP_FILE" "$DATASET_URL"
    echo "Download complete."
else
    echo "Dataset '$ZIP_FILE' already exists. Skipping download."
fi

# Create the txt directory.
echo "Ensuring txt directory '$INPUT_DIR/' exists..."
mkdir -p "$INPUT_DIR"

# Unzip the file and copy into the txt directory.
echo "Unzipping '$ZIP_FILE' into '$INPUT_DIR/'..."
unzip -o "$ZIP_FILE" -d "$INPUT_DIR"

# Run the wordcount script
echo "Running benchmark with '$SCRIPT_NAME'..."
python3 "$SCRIPT_NAME"

echo "Wordcount script complete."
