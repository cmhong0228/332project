#!/bin/bash

TARGET_DIR=$1
VALSORT_CMD=${2:-../64/valsort}

if [ -z "$TARGET_DIR" ]; then
    echo "Usage: $0 <directory_path> [valsort_path]"
    exit 1
fi

if [ ! -x "$VALSORT_CMD" ]; then
    echo "Error: Cannot find executable 'valsort' at: $VALSORT_CMD"
    exit 1
fi

FILES=$(ls $TARGET_DIR/partition.* 2>/dev/null | sort -V)

if [ -z "$FILES" ]; then
    echo "Error: No 'partition.*' files found in $TARGET_DIR"
    exit 1
fi

FILE_ARRAY=($FILES)
FIRST_FILE=${FILE_ARRAY[0]}
LAST_FILE=${FILE_ARRAY[${#FILE_ARRAY[@]}-1]}

echo "Target Directory: $TARGET_DIR"
echo "Processing files..."

# valsort 실행 및 결과 캡처
VALSORT_OUT=$("$VALSORT_CMD" $FILES 2>&1)
VALSORT_EXIT_CODE=$?

if [ $VALSORT_EXIT_CODE -eq 0 ]; then
    echo "----------------------------------------"
    echo "[SUCCESS] Validation Passed!"
    
    # [추가됨] valsort의 원본 출력(Checksum 등)을 여기서 보여줍니다.
    echo "$VALSORT_OUT"
    echo "----------------------------------------"
    
    # 요약 정보 추출
    TOTAL_RECORDS=$(echo "$VALSORT_OUT" | grep "Records:" | awk '{print $2}')
    MIN_KEY_HEX=$(head -c 10 "$FIRST_FILE" | xxd -p -u | tr -d '\n')
    MAX_KEY_HEX=$(tail -c 100 "$LAST_FILE" | head -c 10 | xxd -p -u | tr -d '\n')

    echo "Global Min Key: 0x$MIN_KEY_HEX"
    echo "Global Max Key: 0x$MAX_KEY_HEX"
    echo "----------------------------------------"
else
    echo "----------------------------------------"
    echo "[FAILED] Validation Failed"
    echo "$VALSORT_OUT"
    echo "----------------------------------------"
    exit 1
fi