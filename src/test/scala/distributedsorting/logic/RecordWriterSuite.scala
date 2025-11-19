package distributedsorting.logic

import distributedsorting.distributedsorting.Record
import munit.FunSuite
import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.file.{Files, Path}

class RecordWriterSuite extends FunSuite {
    val RECORD_SIZE: Int = 100
    val KEY_SIZE: Int = 10
    val VALUE_SIZE: Int = RECORD_SIZE - KEY_SIZE
    
    def createTestRecords(count: Int): Seq[Record] = {
        (1 to count).map { i =>
            val key = s"K${i}".getBytes.padTo(KEY_SIZE, 0.toByte) 
            val value = s"V${i}".getBytes.padTo(VALUE_SIZE, 0.toByte)
            key ++ value 
        }
    }

    test("RecordWriter should correctly write all Array[Byte] records using slice for verification") {
        val numRecords = 3
        val records = createTestRecords(numRecords)
        val output = new ByteArrayOutputStream()
        val writer = new RecordWriter(output)

        try {
            writer.writeAll(records.iterator)
        } finally {
            writer.close()
        }
        
        assertEquals(output.size(), numRecords * RECORD_SIZE)

        val writtenBytes = output.toByteArray
        val expectedBytes = records.flatten.toArray
        
        assertEquals(writtenBytes.length, expectedBytes.length, 
                     "The length of written bytes should match the length of expected bytes")
        
        assert(writtenBytes.sameElements(expectedBytes), 
               "The content of written bytes should match the content of expected bytes")
    }

    test("RecordWriter close() must guarantee flush and internal stream close") {
        var closedCount = 0
        var flushedCount = 0
        
        val mockOutput = new OutputStream {
            override def write(b: Int): Unit = {}
            override def flush(): Unit = flushedCount += 1
            override def close(): Unit = closedCount += 1
        }
        
        val writer = new RecordWriter(mockOutput)
        writer.writeAll(Iterator(createTestRecords(1).head))
        writer.close()
        
        assertEquals(flushedCount, 1, "flush should be called by once")
        assertEquals(closedCount, 1, "close should be called by once")
    }
    
    test("RecordWriterRunner should successfully write records to a file") {
        val tempFile: Path = Files.createTempFile("munit-writer-runner", ".dat")
        tempFile.toFile.deleteOnExit()

        val numRecords = 5
        val recordsIterator = createTestRecords(numRecords).iterator

        RecordWriterRunner.WriteRecordIterator(tempFile, recordsIterator)

        assertEquals(Files.size(tempFile), (numRecords * RECORD_SIZE).toLong, "The output file size should match the size of records")

        Files.deleteIfExists(tempFile)
    }
    
    test("RecordWriterRunner must ensure close() is called even on Iterator failure") {
        val tempFile: Path = Files.createTempFile("munit-runner-failure", ".dat")
        tempFile.toFile.deleteOnExit()

        val failingIterator = new Iterator[Record] {
            private var count = 0
            override def hasNext: Boolean = count < 5
            override def next(): Record = {
                count += 1
                if (count == 2) throw new RuntimeException("Simulated Iterator Failure")
                createTestRecords(1).head
            }
        }
        
        intercept[RuntimeException] {
            RecordWriterRunner.WriteRecordIterator(tempFile, failingIterator) 
        }

        assertEquals(Files.size(tempFile), (1*RECORD_SIZE).toLong, "Records should be written until the failure") 
        
        Files.deleteIfExists(tempFile)
    }
}
