#!/bin/bash

# 1. 입력 인자 처리
TARGET_DIR=$1
VALSORT_CMD=${2:-../64/valsort}

if [ -z "$TARGET_DIR" ]; then
    echo "Usage: $0 <directory_path> [valsort_path]"
    echo "Example: $0 data_output"
    exit 1
fi

if [ ! -x "$VALSORT_CMD" ]; then
    echo "Error: Cannot find executable 'valsort' at: $VALSORT_CMD"
    exit 1
fi

# 2. 파일 목록 수집 및 정렬 (가장 중요: 순서 보장)
FILES=$(ls $TARGET_DIR/partition.* 2>/dev/null | sort -V)

if [ -z "$FILES" ]; then
    echo "Error: No 'partition.*' files found in $TARGET_DIR"
    exit 1
fi

FILE_ARRAY=($FILES)
FIRST_FILE=${FILE_ARRAY[0]}
LAST_FILE=${FILE_ARRAY[${#FILE_ARRAY[@]}-1]}
ALL_SUM_FILE="$TARGET_DIR/all_combined.sum"

# 기존 통합 sum 파일이 있다면 삭제
rm -f "$ALL_SUM_FILE"

echo "Target Directory: $TARGET_DIR"
echo "Using valsort at: $VALSORT_CMD"
echo "----------------------------------------"
echo "[Step 1] Validating individual partitions..."

# 3. 개별 파일 검증 및 요약본 합치기 Loop
count=0
for file in "${FILE_ARRAY[@]}"; do
    # 임시 sum 파일명 생성
    temp_sum="${file}.sum"
    
    # [개별 검증] -o 옵션으로 요약 파일 생성
    # -q : quiet 모드 (개별 파일 성공 메시지는 생략하고 에러만 봄)
    "$VALSORT_CMD" -o "$temp_sum" "$file" 2>&1
    RET=$?
    
    if [ $RET -ne 0 ]; then
        echo "[FAILED] Validation failed at file: $file"
        rm -f "$temp_sum" "$ALL_SUM_FILE"
        exit 1
    fi
    
    # [합치기] 생성된 요약 파일을 통합 파일(all.sum)에 이어 붙임 (append)
    # 순서대로 loop를 돌기 때문에 순서 보장됨
    cat "$temp_sum" >> "$ALL_SUM_FILE"
    
    # 임시 파일 삭제 (청소)
    rm -f "$temp_sum"
    
    ((count++))
    # 진행 상황 표시 (선택 사항)
    # echo "  - Processed: $(basename $file)"
done

echo "Successfully validated $count partitions."
echo "----------------------------------------"
echo "[Step 2] Validating global order (summary check)..."

# 4. 최종 통합 검증 (-s 옵션)
# 합쳐진 all.sum 파일로 전체 순서와 체크섬 확인
VALSORT_OUT=$("$VALSORT_CMD" -s "$ALL_SUM_FILE" 2>&1)
VALSORT_EXIT_CODE=$?

# 통합 sum 파일 삭제 (검증 끝났으니 삭제)
rm -f "$ALL_SUM_FILE"

# 5. 결과 출력
if [ $VALSORT_EXIT_CODE -eq 0 ]; then
    echo "[SUCCESS] Global Validation Passed!"
    echo "----------------------------------------"
    echo "$VALSORT_OUT"
    echo "----------------------------------------"
    
    # 요약 정보 추출
    TOTAL_RECORDS=$(echo "$VALSORT_OUT" | grep "Records:" | awk '{print $2}')
    
    # Min Key: 첫 번째 파일의 첫 10바이트
    MIN_KEY_HEX=$(head -c 10 "$FIRST_FILE" | xxd -p -u | tr -d '\n')
    
    # Max Key: 마지막 파일의 마지막 100바이트 중 앞 10바이트
    MAX_KEY_HEX=$(tail -c 100 "$LAST_FILE" | head -c 10 | xxd -p -u | tr -d '\n')

    echo "Total Records : $TOTAL_RECORDS"
    echo "Global Min Key: 0x$MIN_KEY_HEX"
    echo "Global Max Key: 0x$MAX_KEY_HEX"
    echo "----------------------------------------"
else
    echo "[FAILED] Global Validation Failed"
    echo "$VALSORT_OUT"
    echo "----------------------------------------"
    exit 1
fi