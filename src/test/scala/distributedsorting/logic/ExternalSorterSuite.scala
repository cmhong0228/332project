package distributedsorting.logic

import distributedsorting.logic.ExternalSorter
import distributedsorting.distributedsorting._
import munit.FunSuite
import munit.FunFixtures
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Random

object TestIOUtils {
    def writeRecordsToFile(path: Path, records: Iterator[Record]): Unit = {
        Files.createDirectories(path.getParent)
        Files.write(path, records.flatMap(_.toSeq).toArray)
    }
    
    def readAllRecordsFromFile(path: Path): Seq[Record] = {
        val bytes = Files.readAllBytes(path)
        bytes.grouped(8).map(_.toArray).toSeq 
    }

    def readAllRecordsFromDirectory(dirPath: Path): Seq[Array[Byte]] = {
        Files.walk(dirPath).iterator().asScala
            .filter(Files.isRegularFile(_))
            .toSeq
            .sortBy(_.getFileName.toString) 
            .flatMap(path => readAllRecordsFromFile(path))
            .toSeq
    }

    def cleanUpDirectory(path: Path): Unit = {
        if (Files.exists(path)) {
            Files.walk(path).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
        }
    }
}

class TestExternalSorter(ordering: Ordering[Record]) extends ExternalSorter {
    val RECORD_SIZE: Int = 8
    val externalSorterInputDirectory: Path = Paths.get("/dummy/in")
    val externalSorterOutputDirectory: Path = Paths.get("/dummy/out")
    val externalSorterTempDirectory: Path = Paths.get("/dummy/temp")
    override val numMaxMergeGroup: Int = 3
    val chunkSize: Long = 2
    val outputPrefix: String = "partition"
    val outputStartPostfix: Int = 1
    val externalSorterOrdering: Ordering[Record] = ordering
}

class ExternalSorterTestSuite extends FunSuite {

    // 테스트 편의를 위해 Long 값을 Byte 배열 Record로 변환
    // (정렬이 이 Long 값에 의해 결정된다고 가정)
    val RECORD_SIZE = 8
    val KEY_SIZE = 8

    def longToRecord(l: Long): Record = {
        val bytes = new Array[Byte](8) // Key 크기를 8바이트로 가정
        var temp = l
        for (i <- 0 until 8) {
            bytes(7 - i) = (temp & 0xFF).toByte
            temp >>= 8
        }
        bytes
    }

    def recordToLong(k: Record): Long = k.foldLeft(0L)((acc, b) => (acc << 8) | (b & 0xFF))
    
    // Key를 비교할 Ordering 정의: Byte 배열을 Long으로 변환하여 비교
    val externalSorterOrdering: Ordering[Record] = Ordering.by(k => {
        recordToLong(k)
    })
    
    // 테스트 간에 공유할 임시 디렉토리 구조를 위한 FunFixture 정의
    case class TestEnv(base: Path, input: Path, output: Path, temp: Path)
    
    val tempDirFixture: FunFixture[TestEnv] = FunFixture(
        setup = { _ => 
            // Setup: 테스트 전 임시 디렉토리 생성
            val baseDir = Files.createTempDirectory("external_sort_munit_test")
            val inputDir = Files.createDirectories(baseDir.resolve("input"))
            val outputDir = Files.createDirectories(baseDir.resolve("output"))
            val tempDir = Files.createDirectories(baseDir.resolve("temp"))
            
            // 초기 데이터 파일 생성
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block1.data"), Iterator(longToRecord(3L), longToRecord(11L)))
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block2.data"), Iterator(longToRecord(5L), longToRecord(10L)))
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block3.data"), Iterator(longToRecord(9L), longToRecord(12L)))
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block4.data"), Iterator(longToRecord(2L), longToRecord(8L)))
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block5.data"), Iterator(longToRecord(6L), longToRecord(7L)))
            TestIOUtils.writeRecordsToFile(inputDir.resolve("block6.data"), Iterator(longToRecord(1L), longToRecord(4L)))
            
            TestEnv(baseDir, inputDir, outputDir, tempDir)
        }, 
        teardown = { env =>
            // Teardown: 테스트 후 임시 디렉토리 정리
            TestIOUtils.cleanUpDirectory(env.base)
        }
    )

    test("ExternalSorter: splitGroup should partition files correctly based on numMaxMergeGroup") {
        val sorter = new TestExternalSorter(externalSorterOrdering)
        val inputFiles = (1 to 7).map(i => Paths.get(s"file_$i"))
        val groups = sorter.splitGroup(inputFiles) 
        
        assertEquals(groups.size, 3) 
        assertEquals(groups.head.size, 3)
        assertEquals(groups.last.size, 1)
    }

    test("ExternalSorter: iteratorMerge should correctly merge multiple sorted byte array streams") {
        val sorter = new TestExternalSorter(externalSorterOrdering)
        val stream1 = Iterator(longToRecord(1L), longToRecord(3L), longToRecord(4L))
        val stream2 = Iterator(longToRecord(2L), longToRecord(5L), longToRecord(6L))

        val mergedStream = sorter.iteratorMerge(Seq(stream1, stream2)).map(recordToLong(_)).toSeq

        assertEquals(mergedStream, Seq(1L, 2L, 3L, 4L, 5L, 6L))
    }

    tempDirFixture.test("ExternalSorter: getInputFiles should read all files from the input directory") { env =>
        // 통합 테스트용 인스턴스 (I/O 경로가 실제 임시 경로)
        val sorter = new TestExternalSorter(externalSorterOrdering) {
            override val externalSorterInputDirectory: Path = env.input
        }
        val files = sorter.getInputFiles()
        
        assertEquals(files.size, 6)
        assert(files.map(_.getFileName.toString).toSet == Set("block1.data", "block2.data", "block3.data", "block4.data", "block5.data", "block6.data"))
    }

    tempDirFixture.test("ExternalSorter: merge should return the same file if given a single file") { env =>
        val sorter = new TestExternalSorter(externalSorterOrdering) {
        override val externalSorterTempDirectory: Path = env.temp
        }

        // 1. 단일 정렬 파일 생성
        val sortedFile1 = env.temp.resolve("single_file.bin")
        TestIOUtils.writeRecordsToFile(sortedFile1, Iterator(longToRecord(1L), longToRecord(5L)))
        
        val inputFiles = Seq(sortedFile1)
        
        // 2. merge 호출
        val resultPath = sorter.merge(inputFiles)

        // 3. merge()는 루프를 타지 않고 입력 파일과 동일한 경로를 반환해야 함
        assertEquals(resultPath, sortedFile1)
        
        // 4. 내용 확인
        val resultRecords = TestIOUtils.readAllRecordsFromFile(resultPath)
        assertEquals(resultRecords.map(recordToLong), Seq(1L, 5L))
    }

    tempDirFixture.test("ExternalSorter: merge should correctly merge multiple pre-sorted files (multi-pass)") { env =>
        // 1. Sorter 인스턴스 생성
        val sorter = new TestExternalSorter(externalSorterOrdering) {
            override val externalSorterTempDirectory: Path = env.temp
            override val numMaxMergeGroup: Int = 2 
        }

        // 2. 3개의 '미리 정렬된' 입력 파일 생성
        val sortedFile1 = env.temp.resolve("merge_in_1.bin")
        val sortedFile2 = env.temp.resolve("merge_in_2.bin")
        val sortedFile3 = env.temp.resolve("merge_in_3.bin")

        TestIOUtils.writeRecordsToFile(sortedFile1, Iterator(longToRecord(1L), longToRecord(5L), longToRecord(9L)))
        TestIOUtils.writeRecordsToFile(sortedFile2, Iterator(longToRecord(2L), longToRecord(6L), longToRecord(10L)))
        TestIOUtils.writeRecordsToFile(sortedFile3, Iterator(longToRecord(3L), longToRecord(4L), longToRecord(8L)))

        val inputFiles = Seq(sortedFile1, sortedFile2, sortedFile3)
        val expectedLongs = Seq(1L, 2L, 3L, 4L, 5L, 6L, 8L, 9L, 10L)

        // 3. merge 함수 호출
        val resultPath = sorter.merge(inputFiles)

        // 4. 최종 결과 파일의 내용을 읽어 전체 정렬 확인
        val resultRecords = TestIOUtils.readAllRecordsFromFile(resultPath)
        val resultLongs = resultRecords.map(recordToLong)

        assertEquals(resultLongs, expectedLongs)

        // 5. 최종 파일이 임시 디렉토리에 있는지,
        //    그리고 다단계 병합의 결과물(pass-2)인지 확인
        assert(resultPath.startsWith(env.temp), "Result file should be in the temp directory")
        assert(resultPath.getFileName.toString.startsWith("pass-2-group-"), "Should be a multi-pass merge result")
    }

    tempDirFixture.test("ExternalSorter: executeExternalSort should run the full process and produce a fully sorted file") { env =>
        // 통합 테스트용 인스턴스 (I/O 경로가 실제 임시 경로)
        val sorter = new TestExternalSorter(externalSorterOrdering) {
            override val externalSorterInputDirectory: Path = env.input
            override val externalSorterOutputDirectory: Path = env.output
            override val externalSorterTempDirectory: Path = env.temp
            override val numMaxMergeGroup: Int = 2
            override val chunkSize: Long = 2L * 8
        }

        sorter.executeExternalSort()
        
        // 1. 출력 디렉토리에 파일이 존재하는지 검증
        assert(Files.list(env.output).findFirst().isPresent, s"no file in ${sorter.externalSorterOutputDirectory}")

        // 2. 출력 디렉토리의 '모든' 파일을 읽어와 전체 정렬 여부 검증
        val resultRecords = TestIOUtils.readAllRecordsFromDirectory(env.output)
        val resultLongs = resultRecords.map(recordToLong)

        val expectedSortedSeq = Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L)

        assertEquals(resultLongs, expectedSortedSeq)
        
        // 3. 임시 파일 정리 확인 (cleanUpTempFiles는 executeExternalSort 내에서 호출됨)
        assert(!Files.list(env.temp).findFirst().isPresent, s"should clean up temp ${env.temp}")
    }
}