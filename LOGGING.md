# 로깅 설정 가이드

이 프로젝트는 application.conf를 통해 로깅을 쉽게 제어할 수 있습니다.

## 설정 방법

[src/main/resources/application.conf](src/main/resources/application.conf)에서 다음 설정을 변경하세요:

```hocon
distributedsorting {
    logging {
        # 콘솔(stdout)에 로그 출력 여부
        enable-console = true

        # 파일에 로그 출력 여부
        enable-file = false

        # 로그 파일 경로
        file-path = "logs/application.log"

        # 로그 레벨: TRACE, DEBUG, INFO, WARN, ERROR
        level = "INFO"
    }
}
```

## 사용 시나리오

### 1. **개발 중 - 콘솔에만 로그 출력**
```hocon
logging {
    enable-console = true   # 콘솔 O
    enable-file = false     # 파일 X
    level = "DEBUG"         # 상세한 로그
}
```

### 2. **프로덕션 - 파일에만 로그 저장**
```hocon
logging {
    enable-console = false  # 콘솔 X
    enable-file = true      # 파일 O
    file-path = "/var/log/distributedsorting/app.log"
    level = "INFO"          # 일반 로그
}
```

### 3. **디버깅 - 콘솔 + 파일 둘 다**
```hocon
logging {
    enable-console = true   # 콘솔 O
    enable-file = true      # 파일 O
    level = "DEBUG"         # 디버그 레벨
}
```

### 4. **성능 테스트 - 로그 최소화**
```hocon
logging {
    enable-console = false  # 콘솔 X
    enable-file = false     # 파일 X
    level = "ERROR"         # 에러만
}
```

## 로그 레벨 설명

| 레벨 | 설명 | 예시 |
|------|------|------|
| **TRACE** | 가장 상세한 로그 (매우 많음) | 모든 내부 동작 추적 |
| **DEBUG** | 디버깅용 상세 정보 | 함수 호출, 변수 값 등 |
| **INFO** | 일반 정보 (권장) | 단계별 진행 상황 |
| **WARN** | 경고 메시지 | 잠재적 문제 |
| **ERROR** | 에러 메시지 | 실패한 작업 |

## 재빌드 필요

설정 변경 후 재빌드하세요:

```bash
sbt assembly
```

## 테스트

설정 확인:

```bash
# 1. 콘솔 + 파일 로깅 활성화
#    application.conf에서 enable-console=true, enable-file=true 설정

# 2. 재빌드
sbt assembly

# 3. 테스트 실행
./scripts/local-test.sh 3 100000

# 4. 로그 확인
ls -lh logs/application.log
tail -20 logs/application.log
```

## 주의사항

1. **파일 로그 활성화 시 디스크 공간 확인**
   - 대용량 데이터 처리 시 로그 파일도 커질 수 있습니다
   - 필요 없는 로그는 비활성화하세요

2. **성능 영향**
   - DEBUG, TRACE 레벨은 성능에 영향을 줄 수 있습니다
   - 프로덕션에서는 INFO 레벨 권장

3. **로그 회전 (Log Rotation)**
   - 현재는 단순 FileAppender 사용
   - 필요시 RollingFileAppender로 변경 가능

## 트러블슈팅

### 로그가 출력되지 않는 경우

1. application.conf 확인
   ```bash
   cat src/main/resources/application.conf | grep -A 5 "logging"
   ```

2. JAR 파일 재빌드
   ```bash
   sbt clean assembly
   ```

3. 파일 로그가 안 나올 때
   - logs/ 디렉토리 권한 확인
   - file-path 경로가 올바른지 확인

### 로그가 너무 많을 때

```hocon
logging {
    level = "WARN"  # INFO → WARN으로 변경
}
```

## 예제

### 파일만 로그하고 콘솔은 최소화

```hocon
distributedsorting {
    logging {
        enable-console = false
        enable-file = true
        file-path = "logs/run-$(date +%Y%m%d-%H%M%S).log"
        level = "INFO"
    }
}
```

### 디버그 레벨로 상세 로그

```hocon
distributedsorting {
    logging {
        enable-console = true
        enable-file = true
        file-path = "logs/debug.log"
        level = "DEBUG"
    }
}
```
