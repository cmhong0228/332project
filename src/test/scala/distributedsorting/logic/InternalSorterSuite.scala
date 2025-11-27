package distributedsorting.logic

import munit.FunSuite
import java.nio.file.{Files, Path}
import scala.util.Random
import scala.jdk.CollectionConverters._

import distributedsorting.distributedsorting.{Record, createRecordOrdering}

class InternalSorterSuite extends FunSuite {

  // -------------------------
  // 테스트용 구현 클래스 (InternalSorter 원본 로직 100%)
  // -------------------------
  class TestSorter(
                    val internalSorterDirectories: Seq[Path],
                    val ordering: Ordering[Record],
                    val filePivot: Vector[Record],
                    val numOfPar: Int,
                    val internalSortWorkerId: Int,
                    val internalSorterOutputDirectory: Path
                  ) extends InternalSorter

  // -------------------------
  // util
  // -------------------------
  private def tempDir(): Path = {
    val dir = Files.createTempDirectory("internalSorterTest_")
    dir.toFile.deleteOnExit()
    dir
  }

  def createFile(dir: Path, name: String): Path = {
    val f = dir.resolve(name)
    Files.createFile(f)
    f
  }

  // 정수를 받아 10바이트 Key를 가진 레코드로 변환 (앞 4바이트에 Int 저장)
  def intToRecord(i: Int): Record = {
    val bytes = new Array[Byte](100)
    val bb = java.nio.ByteBuffer.wrap(bytes)
    bb.putInt(i) // 앞 4바이트에 정수 기록
    bytes
  }

  // 레코드에서 정수값 추출 (검증용)
  def recordToInt(r: Record): Int = {
    java.nio.ByteBuffer.wrap(r).getInt
  }

  def readRecordsFromFile(path: Path): Seq[Int] = {
    if (Files.size(path) == 0) return Seq.empty
    Files.readAllBytes(path).grouped(100).map(recordToInt).toSeq
  }

  def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder()) // 깊은 곳(파일)부터 거꾸로 나옴
        .map(_.toFile)
        .forEach(_.delete())
    }
  }

  // -------------------------
  // TEST 1: FilePath 생성 기능
  // -------------------------
  // [TEST 1] 다중 디렉토리 및 빈 디렉토리 처리 확인
  test("madeFilePath should collect files from multiple directories and skip empty ones") {
    // Given: 3개의 디렉토리 준비
    val emptyDir = tempDir()      // 1. 빈 디렉토리
    val dirA = tempDir()          // 2. 파일이 있는 디렉토리 A
    val dirB = tempDir()          // 3. 파일이 있는 디렉토리 B

    // 파일 생성
    val fileA1 = createFile(dirA, "data_a1.dat")
    val fileA2 = createFile(dirA, "data_a2.txt")
    val fileB1 = createFile(dirB, "data_b1.dat")

    val sorter = new TestSorter(Seq(emptyDir, dirA, dirB),
      createRecordOrdering(1, 1),
      Vector.empty,
      numOfPar = 3,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = tempDir()
    )

    val resultPaths = sorter.madeFilePath()

    assertEquals(resultPaths.size, 3)

    // 순서 상관없이 경로가 포함되어 있는지 Set으로 비교
    val expectedSet = Set(
      fileA1.toAbsolutePath.toString,
      fileA2.toAbsolutePath.toString,
      fileB1.toAbsolutePath.toString
    )
    val actualSet = resultPaths.map(_.toAbsolutePath.toString).toSet

    assertEquals(actualSet, expectedSet)
    deleteRecursively(emptyDir)
    deleteRecursively(dirA)
    deleteRecursively(dirB)
  }

  // [TEST 2] 서브 디렉토리 무시 확인 (Flat Scan)
  test("madeFilePath should IGNORE sub-directories and files inside them") {
    // Given: 디렉토리 안에 파일과 서브 디렉토리가 섞인 구조
    val rootDir = tempDir()
    
    // 1. 루트 레벨 파일 (읽어야 함)
    val rootFile1 = createFile(rootDir, "valid_root_1.dat")
    val rootFile2 = createFile(rootDir, "valid_root_2.dat")

    // 2. 서브 디렉토리 생성 (무시해야 함)
    val subDir = rootDir.resolve("ignore_me_dir")
    Files.createDirectory(subDir)

    // 3. 서브 디렉토리 안의 파일 (무시해야 함 - 재귀 탐색 금지)
    createFile(subDir, "nested_file_should_be_ignored.dat")

    val sorter = new TestSorter(Seq(rootDir),
      createRecordOrdering(1, 1),
      Vector.empty,
      numOfPar = 3,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = tempDir()
    )

    val resultPaths = sorter.madeFilePath()

    // 루트에 있는 파일 2개만 가져와야 함
    assertEquals(resultPaths.size, 2)

    val resultStrings = resultPaths.map(_.toAbsolutePath.toString)

    // 루트 파일들은 포함되어야 함
    assert(resultStrings.contains(rootFile1.toAbsolutePath.toString))
    assert(resultStrings.contains(rootFile2.toAbsolutePath.toString))

    // 서브 디렉토리 자체나 그 안의 파일은 절대 포함되면 안 됨
    assert(!resultStrings.exists(_.contains("ignore_me_dir")))
    assert(!resultStrings.exists(_.contains("nested_file")))

    deleteRecursively(rootDir)
  }

  // -------------------------
  // TEST 2: madeFile 생성 기능
  // -------------------------
  test("madeFile should generate correct file paths based on workerId, partitionId, and k") {
    // 1. Given: 테스트 환경 설정
    val outDir = tempDir()
    val workerId = 10
    val numPar = 5  // 파티션 5개
    val kIndex = 99 // 파일 인덱스 k

    val sorter = new TestSorter(
      internalSorterDirectories = Seq.empty, 
      ordering = null,
      filePivot = Vector.empty,
      numOfPar = numPar,
      internalSortWorkerId = workerId,
      internalSorterOutputDirectory = outDir
    )

    val resultPaths = sorter.madeFile(kIndex)


    // 개수 검증 (파티션 개수와 일치해야 함)
    assertEquals(resultPaths.size, numPar)

    // 경로 및 이름 규칙 검증
    resultPaths.zipWithIndex.foreach { case (path, index) =>
      // A. 생성된 경로가 지정된 출력 디렉토리(outDir) 안에 있어야 함
      assertEquals(path.getParent, outDir)

      // B. 파일 이름 분석
      val fileName = path.getFileName.toString
      // 예상 포맷: file_{workerId}_{partitionId}_{k}.dat
      // 여기서는 1부터 시작한다고 가정한 코드 (index + 1) -> 만약 0부터면 index 로 수정
      val expectedPrefix = s"file_${workerId}_"
      val expectedSuffix = s"_${kIndex}.dat"

      assert(fileName.startsWith(expectedPrefix), s"Filename should start with $expectedPrefix but was $fileName")
      assert(fileName.endsWith(expectedSuffix), s"Filename should end with $expectedSuffix but was $fileName")
      
      // 중간 파티션 ID 부분 파싱 검증 (더 엄격한 테스트)
      val parts = fileName.split("_")
      // parts 예시: Array("file", "10", "1", "99.dat")
      assertEquals(parts(1).toInt, workerId) // worker ID 확인
      
      // 파티션 ID가 유효한 범위인지 확인
      val partitionId = parts(2).toInt
      assert(partitionId >= 1 && partitionId <= numPar, s"Partition ID $partitionId is out of range")
      
      // k값 확인 (확장자 제거)
      val kInFile = parts(3).replace(".dat", "").toInt
      assertEquals(kInFile, kIndex)
    }
  }

  test("madeFile should generate paths covering ALL partition IDs from 1 to numOfPar exactly once") {
    // 1. Given
    val outDir = tempDir()
    val workerId = 10
    val numPar = 20
    val kIndex = 99

    val sorter = new TestSorter(
      internalSorterDirectories = Seq.empty,
      ordering = null,
      filePivot = Vector.empty,
      numOfPar = numPar,
      internalSortWorkerId = workerId,
      internalSorterOutputDirectory = outDir
    )

    val resultPaths = sorter.madeFile(kIndex)

    assertEquals(resultPaths.size, numPar)

    val extractedPartitionIds = resultPaths.map { path =>
      val fileName = path.getFileName.toString
      // 포맷 가정: file_{workerId}_{partitionId}_{k}.dat
      // split 결과: Array("file", "10", "3", "99.dat") -> 인덱스 2가 파티션 ID
      val parts = fileName.split("_")
      
      // 혹시라도 형식이 틀려서 에러나는 걸 방지하기 위한 안전장치
      assert(parts.length >= 4, s"Invalid filename format: $fileName")
      
      parts(2).toInt
    }

    val expectedIds = (1 to numPar).toList
    
    assertEquals(extractedPartitionIds.sorted, expectedIds)
  }

  // -------------------------
  // TEST 3: partition 테스트
  // -------------------------
  // [TEST 1] 일반적인 경우 & 경계값 테스트 (Pivot 값이 뒤쪽 파티션으로 이동)
  test("partition should split records based on pivots (Pivot value goes to NEXT partition)") {
    // Given: 파티션 3개, 피벗 2개 (10, 20)
    // 범위 예상:
    // P0: x < 10
    // P1: 10 <= x < 20 (10 포함)
    // P2: 20 <= x      (20 포함)
    val pivots = Vector(intToRecord(10), intToRecord(20))

    val sorter = new TestSorter(
      Seq.empty, 
      createRecordOrdering(10, 100), 
      pivots, 
      numOfPar = 3, 
      0, 
      tempDir()
    )

    // 입력: 5(P0), 10(P1 경계), 15(P1), 20(P2 경계), 25(P2)
    val input = Seq(5, 10, 15, 20, 25).map(intToRecord)

    val result = sorter.partition(input)

    assertEquals(result.size, 3)

    val resultInts = result.map(_.map(recordToInt))

    assertEquals(resultInts(0), Seq(5))
    assertEquals(resultInts(1), Seq(10, 15))
    assertEquals(resultInts(2), Seq(20, 25))
  }

  // [TEST 2] 데이터 쏠림 현상 (Empty Partition 발생)
  test("partition should handle skewed data (leaving some partitions empty)") {
    // Given: 모든 데이터가 10보다 작음 -> 모두 P0으로 가야 함
    val pivots = Vector(intToRecord(10), intToRecord(20))
    val sorter = new TestSorter(
      Seq.empty, createRecordOrdering(10, 100), pivots, 
      numOfPar = 3, 0, tempDir()
    )

    val input = Seq(1, 2, 3).map(intToRecord)

    val result = sorter.partition(input)
    val resultInts = result.map(_.map(recordToInt))

    assertEquals(result.size, 3)
    assertEquals(resultInts(0), Seq(1, 2, 3))
    assert(resultInts(1).isEmpty)
    assert(resultInts(2).isEmpty)
  }

  // [TEST 3] 역방향 쏠림 현상 (모두 큰 값인 경우)
  test("partition should handle data skewed to the last partition") {
    // Given: 모든 데이터가 20보다 큼 -> 모두 P2로 가야 함
    val pivots = Vector(intToRecord(10), intToRecord(20))
    val sorter = new TestSorter(
      Seq.empty, createRecordOrdering(10, 100), pivots, 
      numOfPar = 3, 0, tempDir()
    )

    val input = Seq(30, 40, 50).map(intToRecord)

    val result = sorter.partition(input)
    val resultInts = result.map(_.map(recordToInt))

    assertEquals(result.size, 3)
    assert(resultInts(0).isEmpty)
    assert(resultInts(1).isEmpty)
    assertEquals(resultInts(2), Seq(30, 40, 50))
  }

  // [TEST 4] 입력 데이터가 없는 경우
  test("partition should return a sequence of empty sequences for empty input") {
    // Given
    val pivots = Vector(intToRecord(10)) // 파티션 2개 가정
    val sorter = new TestSorter(
      Seq.empty, createRecordOrdering(10, 100), pivots, 
      numOfPar = 2, 0, tempDir()
    )

    // When
    val result = sorter.partition(Seq.empty)

    // Then
    assertEquals(result.size, 2)
    assert(result(0).isEmpty)
    assert(result(1).isEmpty)
  }

  // -------------------------
  // TEST 4: saveFile 테스트 (실제 파일에 기록)
  // -------------------------
  test("saveFile should write partitioned records to corresponding files correctly") {
    // 1. Given: 환경 및 데이터 설정
    val outDir = tempDir()
    
    // TestSorter 초기화 (saveFile은 내부 상태값보다는 인자를 주로 사용하므로 기본 설정)
    val sorter = new TestSorter(
      Seq.empty, null, Vector.empty, 
      numOfPar = 3, internalSortWorkerId = 0, internalSorterOutputDirectory = outDir
    )

    // 테스트 데이터 준비 (3개의 파티션)
    // Partition 0: 데이터 2개 (100, 200)
    // Partition 1: 데이터 없음 (빈 파티션 테스트)
    // Partition 2: 데이터 1개 (300)
    val p0Data = Seq(100, 200).map(intToRecord)
    val p1Data = Seq.empty[Record]
    val p2Data = Seq(300).map(intToRecord)
    
    val partitionResult: Seq[Seq[Record]] = Seq(p0Data, p1Data, p2Data)

    // 저장할 파일 경로 준비 (saveFile의 두 번째 인자)
    val file0 = outDir.resolve("partition_0.dat")
    val file1 = outDir.resolve("partition_1.dat")
    val file2 = outDir.resolve("partition_2.dat")
    val outputPaths = List(file0, file1, file2)

    // 2. When: 파일 저장 실행
    sorter.saveFile(partitionResult, outputPaths)

    // 3. Then: 검증

    // [검증 1] 파일 생성 여부 확인
    assert(Files.exists(file0), "File 0 should exist")
    assert(Files.exists(file1), "File 1 should exist (even if empty)")
    assert(Files.exists(file2), "File 2 should exist")

    // [검증 2] 파일 크기 확인 (레코드 크기가 10바이트라고 가정 시)
    val recordSize = 100
    assertEquals(Files.size(file0), (2 * recordSize).toLong) // 2 records
    assertEquals(Files.size(file1), 0L)                      // 0 records
    assertEquals(Files.size(file2), (1 * recordSize).toLong) // 1 record

    // [검증 3] 데이터 내용 일치 확인 (Read Back check)
    // File 0 읽어서 확인
    val bytes0 = Files.readAllBytes(file0)
    val records0 = bytes0.grouped(recordSize).map(recordToInt).toSeq
    assertEquals(records0, Seq(100, 200))

    // File 2 읽어서 확인
    val bytes2 = Files.readAllBytes(file2)
    val records2 = bytes2.grouped(recordSize).map(recordToInt).toSeq
    assertEquals(records2, Seq(300))

    deleteRecursively(outDir)
  }

  // -------------------------
  // TEST 5: runSortAndPartition 통합 테스트
  // -------------------------
  test("runSortAndPartition should read ALL inputs, sort, partition, and write to output files") {
    // 1. Given: 환경 설정
    val inputDir1 = tempDir()
    val inputDir2 = tempDir()
    val outDir = tempDir()

    // 피벗 설정: 10, 20
    // P0: < 10, P1: 10 <= x < 20, P2: 20 <= x
    val pivots = Vector(intToRecord(10), intToRecord(20))
    val numPar = 3

    val sorter = new TestSorter(
      internalSorterDirectories = Seq(inputDir1, inputDir2),
      ordering = createRecordOrdering(10, 100),
      filePivot = pivots,
      numOfPar = numPar,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = outDir
    )

    // 2. 입력 파일 생성 (정렬되지 않은 데이터)
    // Input 1: [15, 5, 25]
    val data1 = Seq(15, 5, 25).map(intToRecord)
    val file1 = inputDir1.resolve("input_1.dat")
    // 레코드들을 바이트 배열 하나로 합쳐서 파일에 쓰기
    Files.write(file1, data1.reduce(_ ++ _)) 

    // Input 2: [8, 12, 22]
    val data2 = Seq(8, 12, 22).map(intToRecord)
    val file2 = inputDir2.resolve("input_2.dat")
    Files.write(file2, data2.reduce(_ ++ _))

    // 3. When: 통합 함수 실행
    val resultPaths = sorter.runSortAndPartition()

    // 4. Then: 검증
    assertEquals(resultPaths.size, 2 * numPar)

    val filesByInputIndex = resultPaths.groupBy { path =>
      val fileName = path.getFileName.toString
      val parts = fileName.split("_")
      parts.last.replace(".dat", "").toInt
    }

    val sortedIndices = filesByInputIndex.keys.toSeq.sorted

    assertEquals(sortedIndices.size, 2, "Should detect exactly 2 input sources")

    val firstIndex = sortedIndices(0)
    val secondIndex = sortedIndices(1)

    // 첫 번째 인덱스 그룹 검증
    val filesForFirst = filesByInputIndex(firstIndex).sortBy(_.getFileName.toString)
    assertEquals(readRecordsFromFile(filesForFirst(0)), Seq(5))
    assertEquals(readRecordsFromFile(filesForFirst(1)), Seq(15))
    assertEquals(readRecordsFromFile(filesForFirst(2)), Seq(25))

    // 두 번째 인덱스 그룹 검증
    val filesForSecond = filesByInputIndex(secondIndex).sortBy(_.getFileName.toString)
    assertEquals(readRecordsFromFile(filesForSecond(0)), Seq(8))
    assertEquals(readRecordsFromFile(filesForSecond(1)), Seq(12))
    assertEquals(readRecordsFromFile(filesForSecond(2)), Seq(22))

    deleteRecursively(inputDir1)
    deleteRecursively(inputDir2)
    deleteRecursively(outDir)
  }
}
