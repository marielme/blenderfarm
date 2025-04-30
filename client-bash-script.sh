#!/bin/bash
# Blender Render Farm Client Starter Script
# Usage: ./start_client.sh [CLIENT_NAME]

# Default client name
CLIENT_NAME="Client-$(hostname)"
if [ $# -eq 1 ]; then
    CLIENT_NAME="$1"
fi

# Find Blender executable
BLENDER="blender"
if [ ! -x "$(command -v $BLENDER)" ]; then
    # Try common Blender paths
    COMMON_PATHS=(
        "/Applications/Blender.app/Contents/MacOS/Blender"
        "/usr/bin/blender"
        "/usr/local/bin/blender"
        "/opt/blender/blender"
    )
    
    for path in "${COMMON_PATHS[@]}"; do
        if [ -x "$path" ]; then
            BLENDER="$path"
            break
        fi
    done
fi

if [ ! -x "$(command -v $BLENDER)" ]; then
    echo "Error: Blender executable not found"
    echo "Please edit this script and set the correct path to Blender"
    exit 1
fi

echo "Starting Blender Render Farm Client: $CLIENT_NAME"
$BLENDER --background --python client.py -- --name "$CLIENT_NAME"