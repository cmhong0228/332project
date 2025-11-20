#!/bin/bash

# ==========================================
# Gensort Wrapper Script
# ==========================================

# 1. Default Settings
DEFAULT_GENSORT_PATH="../64/gensort"
GENSORT_BIN="$DEFAULT_GENSORT_PATH"

IS_ASCII=false           # Default: Binary
NUM_RECORDS=1000         # Records per file
START_OFFSET=0           # Start index
FILES_PER_DIR=1          # Files per directory
DO_RESET=false           # Reset output directory (Default: Keep files)

# Usage Function
usage() {
    echo "Usage: $0 [-g path] [-a] [-n count] [-s offset] [-c count] [-r] <output_directories...>"
    echo "  -g path   : Path to gensort executable (Default: $DEFAULT_GENSORT_PATH)"
    echo "  -a        : Generate ASCII data (Default: Binary)"
    echo "  -n count  : Number of records per file (Default: 1000)"
    echo "  -s offset : Start offset (Default: 0)"
    echo "  -c count  : Number of files per directory (Default: 1)"
    echo "  -r        : Reset (clear) output directories before generating"
    exit 1
}

# 2. Parse Options (added 'r')
while getopts "g:an:s:c:r" opt; do
    case "$opt" in
        g) GENSORT_BIN="$OPTARG" ;;
        a) IS_ASCII=true ;;
        n) NUM_RECORDS="$OPTARG" ;;
        s) START_OFFSET="$OPTARG" ;;
        c) FILES_PER_DIR="$OPTARG" ;;
        r) DO_RESET=true ;;
        *) usage ;;
    esac
done

shift $((OPTIND - 1))

# Check for output directories
if [ $# -eq 0 ]; then
    echo "Error: Please specify at least one output directory."
    usage
fi

# Check executable
if [ ! -f "$GENSORT_BIN" ]; then
    echo "Error: '$GENSORT_BIN' not found."
    echo "   Please check the path or use -g to specify the correct location."
    exit 1
fi

# Set Flags
GENSORT_FLAGS=""
if [ "$IS_ASCII" = true ]; then
    GENSORT_FLAGS="-a"
fi

# ==========================================
# 3. Main Logic
# ==========================================

OUTPUT_DIRS=("$@")
CURRENT_OFFSET=$START_OFFSET
FILE_ID_COUNTER=1

echo "=== Starting Gensort Data Generation ==="
echo "Executable: $GENSORT_BIN"
if [ "$IS_ASCII" = true ]; then echo "Mode: ASCII"; else echo "Mode: Binary"; fi
if [ "$DO_RESET" = true ]; then echo "Option: Reset output directories"; fi
echo "Records per file: $NUM_RECORDS"
echo "----------------------------------------"

for dir in "${OUTPUT_DIRS[@]}"; do
    
    # [New Feature] Reset Directory if -r is passed
    if [ "$DO_RESET" = true ] && [ -d "$dir" ]; then
        echo "[$dir] Cleaning up existing files (-r option)..."
        rm -rf "$dir"
    fi

    # Create directory if it doesn't exist (or was just deleted)
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
    fi

    for ((i=0; i<FILES_PER_DIR; i++)); do
        FILENAME="$dir/input_partition$FILE_ID_COUNTER"

        CMD="$GENSORT_BIN $GENSORT_FLAGS -b$CURRENT_OFFSET $NUM_RECORDS $FILENAME"
        
        echo "[$dir] Generating... input_partition$FILE_ID_COUNTER (Offset: $CURRENT_OFFSET)"
        $CMD

        CURRENT_OFFSET=$((CURRENT_OFFSET + NUM_RECORDS))
        FILE_ID_COUNTER=$((FILE_ID_COUNTER + 1))
    done
done

echo "----------------------------------------"
echo "Done!"
echo "Next Offset: $CURRENT_OFFSET"