package distributedsorting.logic

import distributedsorting.distributedsorting.Record
import munit.FunSuite
import java.io._
import java.nio.file.{Files, Path, Paths}

trait RecordIteratorTestSetup {
    val RECORD_SIZE = 8
    val NUM_RECORDS = 10
    val TOTAL_BYTES = RECORD_SIZE * NUM_RECORDS

    val testData: Array[Byte] = (1 to NUM_RECORDS).flatMap { i =>
        Array.fill[Byte](RECORD_SIZE)((i + 10).toByte) 
    }.toArray

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
            var count = 0
            while (iterator.hasNext) {
                val record = iterator.next()
                assertEquals(record.length, RECORD_SIZE)

                val startIndex = count * RECORD_SIZE 
                val expectedRecord = testData.slice(startIndex, startIndex + RECORD_SIZE)
                
                assert(record.sameElements(expectedRecord), s"Record ${count} content mismatch.")

                count += 1
            }
            assertEquals(count, NUM_RECORDS)
            
            intercept[NoSuchElementException] {
                iterator.next()
            }
        } finally {
            iterator.close()
        }
    }

    test("Stream: Incomplete record should throw Exception") {
        val incompleteData = testData.take(TOTAL_BYTES - 1) 
        val is = new ByteArrayInputStream(incompleteData)
        val iterator = new StreamRecordIterator(is, RECORD_SIZE)

        (1 until NUM_RECORDS).foreach(_ => iterator.next())

        intercept[NoSuchElementException] {
            iterator.next()
        }
        
        iterator.close()
    }
    
    test("Stream: close() should close the underlying stream") {
        val is = new ByteArrayInputStream(testData) {
            var closedFlag: Boolean = false

            def isClosed: Boolean = closedFlag

            override def close(): Unit = {
                closedFlag = true
                super.close()
            }
        }
        
        val iterator = new StreamRecordIterator(is, RECORD_SIZE)
        
        iterator.close() 

        assert(is.isClosed) 
        
        iterator.close()
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
                val recordsRead = iterator.toList
                
                assertEquals(recordsRead.size, NUM_RECORDS)
                assert(recordsRead.head.sameElements(testData.take(RECORD_SIZE))) 

            } finally {
                iterator.close()
            }
        }
    }

    test("File: close() should ensure file handle is released") {
        withTempFile { filePath =>
            val iterator = new FileRecordIterator(filePath, RECORD_SIZE, 4096)
            
            iterator.next() 
            
            iterator.close() 
            
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

            val outputString = 
            s"""
Conf: Negative flag should load positive values from classpath config
  [$status] $name:
    Expected: $expected
    Actual:   $actual"""
            println(outputString)
        }
        
        withTempFile { filePath =>
            val iterator = new FileRecordIterator(
                filePath, 
                recordSize = -1, 
                bufferSize = -1 
            ) with FileRecordIteratorWithAccessors
            
            val loadedRecordSize = iterator.getTestRecordSize
            assert(loadedRecordSize > 0, "Record Size must be a positive value.")
            logAndVerify("Record Size", loadedRecordSize, EXPECTED_RECORD_SIZE)
            
            val loadedBufferSize = iterator.getTestBufferSize
            assert(loadedBufferSize > 0, "Buffer Size must be a positive value.")
            logAndVerify("Buffer Size", loadedBufferSize, EXPECTED_BUFFER_SIZE)

            iterator.close()
        }
    }
}

class RecordIteratorSuite 
    extends FunSuite 
    with StreamIteratorTests 
    with FileIteratorTests {
    
}
