#!/bin/bash

# Check if the first argument is provided
if [ $# -eq 0 ]; then
    echo "Error: No argument provided."
    exit 1
fi

# Get first argument as pattern
pattern="$1"
shift

# Check if there's a command to execute
if [ $# -eq 0 ]; then
    echo "Error: No team provided."
    exit 1
fi

# Store the remaining arguments as the command to execute
team="$1"

# Find all files matching the pattern, sort them, and iterate
for file in $(find . -name "$pattern" | sort); do
  echo "mattermost import slack $team $file"
done
