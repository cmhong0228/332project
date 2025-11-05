package distributedsorting.logic

import distributedsorting.distributedsorting._
import munit.FunSuite
import java.nio.file.{Path, Files}

trait SmaplerTests extend FunSuite with Sampler{
    val KEY_SIZE = 10

    test("Sampler: sampleKeys should yield expected mean sample size within 10% margin of error") {        
        val totalRecords = 10000 // 총 레코드 수 (모집단)
        val targetRatio = 0.01  // 목표 샘플링 비율
        val numRuns = 200        // 테스트 반복 횟수 (평균 정확도를 높임)
        
        val expectedSampleSize = totalRecords * targetRatio // 기대값
        // 허용 오차 범위 설정
        val allowedErrorRatio = 0.1
        val allowedError = expectedSampleSize * allowedErrorRatio
        
        var totalActualSamples = 0.0

        // 반복 실행하여 평균을 계산
        for (_ <- 1 to numRuns) {
            // 매번 새로운 레코드 스트림 생성
            val inputRecords = (1 to totalRecords).map(i => Array.fill[Byte](KEY_SIZE)(i.toByte)).iterator
            val samples = sampleKeys(inputRecords, targetRatio)
            totalActualSamples += samples.size
        }

        val meanSampleSize = totalActualSamples / numRuns
        
        // 1. 하한 검증
        val lowerBound = expectedSampleSize - allowedError
        assert(meanSampleSize >= lowerBound, 
            s"Mean sample size ($meanSampleSize) is below lower bound ($lowerBound).")
            
        // 2. 상한 검증
        val upperBound = expectedSampleSize + allowedError
        assert(meanSampleSize <= upperBound,
            s"Mean sample size ($meanSampleSize) is above upper bound ($upperBound).")
            
        // 디버깅 및 출력: 통과해도 평균 크기를 출력하여 확인
        println(s"\n[Sampling Test Result] Expected: $expectedSampleSize, Mean Actual: $meanSampleSize")
    }

    test("Sampler: sampled keys must be a subset of input records (Integrity Test)") {
        val totalRecords = 100 
        val targetRatio = 0.5
        
        val originalRecords: Seq[Record] = (1 to totalRecords).map { i => 
            Array.fill[Byte](KEY_SIZE)(i.toByte) 
        }
        val originalRecordSet: Set[Seq[Byte]] = originalRecords.map(_.toSeq).toSet 

        val inputRecords: Iterator[Record] = originalRecords.iterator
        
        val samples = sampler.sampleKeys(inputRecords, targetRatio)
        
        samples.foreach { sampleKey =>
            val sampleKeySeq = sampleKey.toSeq 
            assert(originalRecordSet.contains(sampleKeySeq), 
                s"Sampled key (head: ${sampleKey.head}) was not found in the original input set.")
        }
    }
}

trait RecordCountCalculatorTests extends munit.FunSuite with RecordCountCalculator{
    val RECORD_SIZE = 100

    // 임시 디렉토리에 특정 크기의 더미 파일을 생성하는 함수
    def createDummyFile(dir: Path, name: String, sizeBytes: Long): Path = {
        val filePath = dir.resolve(name)
        // 파일 크기만큼의 0 바이트를 씁니다.
        Files.write(filePath, Array.fill[Byte](sizeBytes.toInt)(0)) 
        filePath
    }

    test("RecordCountCalculator: calculateTotalRecords should sum file sizes and divide by RECORD_SIZE") {
        
        // munit의 임시 디렉토리 기능을 사용합니다.
        val tempDir1 = tempDirectory()
        val tempDir2 = tempDirectory()
        
        createDummyFile(tempDir1, "data_a", 10000) 
        createDummyFile(tempDir1, "data_b", 20000) 
        createDummyFile(tempDir2, "data_c", 5000)

        val inputDirs = Seq(tempDir1, tempDir2)
        val totalRecords = calculateTotalRecords(inputDirs)

        assertEquals(totalRecords, 350L)
    }

    test("RecordCountCalculator: calculateTotalRecords should return 0 for empty input directories") {        
        val totalRecords = calculateTotalRecords(Seq.empty[Path])

        assertEquals(totalRecords, 0L)
    }

    test("RecordCountCalculator: calculateTotalRecords should handle directories with no files") {        
        val tempDir = tempDirectory() // 파일이 없는 빈 디렉토리

        val totalRecords = calculateTotalRecords(Seq(tempDir))

        assertEquals(totalRecords, 0L)
    }
}

trait RecordExtractorTests extends munit.FunSuite with RecordExtractor { 
    val KEY_SIZE = 10
        
    // 임시 디렉토리에 더미 파일 생성 (RECORD_SIZE의 배수 크기)
    def createDummyFile(dir: Path, name: String, numRecords: Int): Seq[Array[Byte]] = {
        val totalBytes = numRecords * KEY_SIZE
        val dummyRecords = (0 until numRecords).map { i => 
            Array.fill[Byte](RECORD_SIZE)(i.toByte)
        }
        
        val filePath = dir.resolve(name)
        Files.write(filePath, dummyRecords.flatten.toArray) 
        dummyRecords
    }


    test("RecordExtractor: readAndExtractSamples should return all records when samplingRatio is 1.0") {
        
        val targetRatio = 1.0 // 모든 레코드를 추출해야 함
        
        // munit의 임시 디렉토리를 사용
        val tempDir1 = tempDirectory()
        val tempDir2 = tempDirectory()
        
        val records1 = createDummyFile(tempDir1, "data_a", 100) 
        val records2 = createDummyFile(tempDir1, "data_b", 150) 
        val records3 = createDummyFile(tempDir2, "data_c", 250) 
        
        val inputDirs = Seq(tempDir1, tempDir2)
        val expectedTotalRecords = 100 + 150 + 250
        
        val extractedKeys = extractor.readAndExtractSamples(inputDirs, targetRatio)

        assertEquals(extractedKeys.size.toLong, expectedTotalRecords.toLong)
        
        assert(extractedKeys.forall(_.length == KEY_SIZE))
    }
}

trait PivotSelectorTests extends munit.FunSuite with PivotSelector {
    val KEY_SIZE = 8
    val numWorkers = 10

    // 테스트 편의를 위해 Long 값을 Byte 배열 Key로 변환
    // (정렬이 이 Long 값에 의해 결정된다고 가정)
    def longToKey(l: Long): Key = {
        val bytes = new Array[Byte](8) // Key 크기를 8바이트로 가정
        var temp = l
        for (i <- 0 until 8) {
            bytes(7 - i) = (temp & 0xFF).toByte
            temp >>= 8
        }
        bytes
    }

    def keyToLong(k: Key): Long = k.foldLeft(0L)((acc, b) => (acc << 8) | (b & 0xFF))
    
    // Key를 비교할 Ordering 정의: Byte 배열을 Long으로 변환하여 비교
    val ordering: Ordering[Key] = Ordering.by(k => {
        keyToLong(k)
    })

    test("PivotSelector: SortSamples should correctly sort keys based on the provided Ordering") {
        val unsortedKeys: Seq[Key] = Seq(
            longToKey(50), longToKey(10), longToKey(80), longToKey(30)
        )
        
        val expectedSorted: Seq[Key] = Seq(
            longToKey(10), longToKey(30), longToKey(50), longToKey(80)
        )

        val sortedKeys = sortSamples(unsortedKeys)

        assertEquals(sortedKeys.map(_.toSeq), expectedSorted.map(_.toSeq))
    }
    
    test("PivotSelector: selectPivots should choose pivots to minimize partition size imbalance (Max size diff <= 1)") {
        val numRecords = 117
        val sortedKeys: Seq[Key] = (0L until numRecords.toLong).map(longToKey)
        val numRecordsPerPartition = numRecords/numWorkers
        
        val pivotsInLong = selectPivots(sortedKeys).map(keyToLong(_))
        val appendedPivotsInLong = 0L +: pivotsInLong :+ numRecords.toLong
        
        appendedPivotsInLong.sliding(2).foreach {
            pair=>
            val curr = pair.head
            val next = pair.last
            val diff = next-current
            assert(diff == numRecordsPerPartition || diff = numRecordsPerPartition + 1)
        }        
    }
    
    test("PivotSelector: selectPivots should return empty vector if sample size is less than numWorkers") {
        val sortedKeys: Seq[Key] = (1L to 8L).map(longToKey)
        
        val pivots = selectPivots(sortedKeys)
        
        assert(pivots.isEmpty)
    }
}

class SampleLogicSuite extends FunSuite with SmaplerTests with RecordCountCalculatorTests with RecordExtractorTests with PivotSelectorTests {
    
}