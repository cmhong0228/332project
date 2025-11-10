package distributedsorting.logic

import distributedsorting.distributedsorting.Record
import munit.FunSuite
import java.io._
import java.nio.file.{Files, Path, Paths}

trait RecordIteratorTestSetup {
    val RECORD_SIZE = 8 // 레코드 크기
    val NUM_RECORDS = 10  // 테스트에 사용할 레코드 수
    val TOTAL_BYTES = RECORD_SIZE * NUM_RECORDS // 총 바이트 수

    // 테스트 레코드 데이터
    val testData: Array[Byte] = (1 to NUM_RECORDS).flatMap { i =>
        Array.fill[Byte](RECORD_SIZE)((i + 10).toByte) 
    }.toArray

    // 임시 파일 생성 (FileRecordIterator 테스트용)
    def withTempFile(testBody: Path => Any): Any = {
        val tempDir = Files.createTempDirectory("record_iter_test")
        val tempFile = tempDir.resolve("test_data.bin")
        try {
            Files.write(tempFile, testData)
            testBody(tempFile)
        } finally {
            Files.deleteIfExists(tempFile)
            Files.deleteIfExists(tempDir)
        }
    }
}

trait StreamIteratorTests extends RecordIteratorTestSetup { this: FunSuite =>
    test("Stream: Basic iteration and consumption") {
        val is = new ByteArrayInputStream(testData)
        val iterator = new StreamRecordIterator(is, RECORD_SIZE)
        
        try {
            // 1. hasNext와 next 검증
            var count = 0
            while (iterator.hasNext) {
                val record = iterator.next()

                // record 길이 확인
                assertEquals(record.length, RECORD_SIZE)

                // testData에서 현재 레코드의 시작 인덱스 계산
                val startIndex = count * RECORD_SIZE 
                // testData에서 해당 레코드 부분만 추출
                val expectedRecord = testData.slice(startIndex, startIndex + RECORD_SIZE)
                
                // 배열의 내용이 같은지 확인 (Scala의 Array.sameElements 사용)
                assert(record.sameElements(expectedRecord), s"Record ${count} content mismatch.")

                count += 1
            }
            assertEquals(count, NUM_RECORDS)
            
            // 2. 더 이상 next를 호출할 수 없는지 검증
            intercept[NoSuchElementException] {
                iterator.next()
            }
        } finally {
            iterator.close()
        }
    }

    test("Stream: Incomplete record should throw Exception") {
        // 데이터 끝에 1바이트 부족한 불완전한 레코드를 추가
        val incompleteData = testData.take(TOTAL_BYTES - 1) 
        val is = new ByteArrayInputStream(incompleteData)
        val iterator = new StreamRecordIterator(is, RECORD_SIZE)

        // 모든 레코드를 소비 (NUM_RECORDS - 1)
        (1 until NUM_RECORDS).foreach(_ => iterator.next())

        // 마지막 next() 호출 시 불완전 레코드를 읽고 예외 발생
        intercept[NoSuchElementException] {
            iterator.next()
        }
        
        // 리소스를 닫기 (close가 inputStream을 닫으므로)
        iterator.close()
    }
    
    test("Stream: close() should close the underlying stream") {
        // 1. InputStream을 익명 클래스로 확장하고, 상태 확인 메서드를 추가
        val is = new ByteArrayInputStream(testData) {
            var closedFlag: Boolean = false // 플래그를 private이 아닌 상태로 정의

            // 상태를 노출하는 public 메서드 (is.isClosed() 역할)
            def isClosed: Boolean = closedFlag

            override def close(): Unit = {
                closedFlag = true // 플래그를 true로 설정
                super.close()     // 부모 클래스의 close 로직 실행
            }
        }
        
        // 2. Iterator 생성 및 close 호출
        val iterator = new StreamRecordIterator(is, RECORD_SIZE)
        
        // 데이터를 읽지 않아도 close는 호출되어야 함을 검증
        iterator.close() 

        // 3. 검증 (Assertion)
        // iterator.close()가 is.close()를 호출했다면, is.isClosed는 true
        assert(is.isClosed) 
        
        // 4. 닫힌 후 다시 close 호출해도 오류가 없어야 함
        iterator.close() // 닫힌 스트림을 다시 닫아도 안전해야 함 (idempotency)
    }
}

trait FileIteratorTests extends RecordIteratorTestSetup { this: FunSuite =>
    private[logic] trait FileRecordIteratorWithAccessors { 
        this: FileRecordIterator => 
        private[logic] def getTestRecordSize: Int = this.RECORD_SIZE
        private[logic] def getTestBufferSize: Int = this.BUFFER_SIZE
    }

    test("File: Iterator should read from a temporary file") {
        withTempFile { filePath =>
            val iterator = new FileRecordIterator(filePath, RECORD_SIZE, 4096)
            
            try {
                val recordsRead = iterator.toList // 모든 레코드를 소비
                
                assertEquals(recordsRead.size, NUM_RECORDS)
                // 첫 번째 레코드가 올바르게 읽혔는지 검증
                assert(recordsRead.head.sameElements(testData.take(RECORD_SIZE))) 

            } finally {
                iterator.close()
            }
        }
    }

    test("File: close() should ensure file handle is released") {
        // close() 호출 후 next()를 호출했을 때 예외가 발생하는지 확인
        withTempFile { filePath =>
            val iterator = new FileRecordIterator(filePath, RECORD_SIZE, 4096)
            
            // 데이터를 일부 읽기
            iterator.next() 
            
            // 닫기
            iterator.close() 
            
            // 닫힌 후에는 next() 호출 시 예외 발생
            // (내부 스트림이 닫혔으므로 IOException/RuntimeException이 delegate에서 발생)
            intercept[Exception] { 
                iterator.next() 
            }
        }
    }
    
    test("Conf: Negative flag should load positive values from classpath config") {
        val EXPECTED_RECORD_SIZE = 100
        val EXPECTED_BUFFER_SIZE = 8192

        def logAndVerify(name: String, actual: Int, expected: Int): Unit = {
            val success = actual == expected
            val status = if (success) "SUCCESS" else "FAILURE"

            // 일치 여부와 값을 상세하게 출력
            println("Conf: Negative flag should load positive values from classpath config")
            println(s"  [$status] $name:")
            println(s"      Expected: $expected")
            println(s"      Actual:   $actual")
        }
        
        withTempFile { filePath =>
            val iterator = new FileRecordIterator(
                filePath, 
                recordSize = -1, 
                bufferSize = -1 
            ) with FileRecordIteratorWithAccessors // 테스트를 위해 믹스인
            
            // 1. Record Size 검증 및 출력
            val loadedRecordSize = iterator.getTestRecordSize
            assert(loadedRecordSize > 0, "Record Size must be a positive value.")
            logAndVerify("Record Size", loadedRecordSize, EXPECTED_RECORD_SIZE)
            
            // 2. Buffer Size 검증 및 출력
            val loadedBufferSize = iterator.getTestBufferSize
            assert(loadedBufferSize > 0, "Buffer Size must be a positive value.")
            logAndVerify("Buffer Size", loadedBufferSize, EXPECTED_BUFFER_SIZE)

            iterator.close()
        }
    }
}

/**
 * StreamIteratorTests와 FileIteratorTests를 모두 실행하는 통합 테스트 클래스
 */
class RecordIteratorSuite 
    extends FunSuite 
    with StreamIteratorTests 
    with FileIteratorTests {
    
}