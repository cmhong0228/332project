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

  private def record(bytes: Byte*): Record =
    bytes.toArray

  // -------------------------
  // TEST 1: FilePath 생성 기능
  // -------------------------
  test("madeFilePath should produce correct file paths") {
    val inputDir = tempDir()
    val sorter = new TestSorter(
      Seq(inputDir),
      createRecordOrdering(2, 4),
      Vector.empty,
      numOfPar = 2,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = tempDir()
    )

    val paths = sorter.madeFilePath()
    assert(paths.nonEmpty)
    assert(paths.exists(_.getFileName.toString.contains("block_0.dat")))
  }

  // -------------------------
  // TEST 2: madeFile 생성 기능
  // -------------------------
  test("madeFile should generate output file names correctly") {
    val outDir = tempDir()
    val sorter = new TestSorter(
      Seq(tempDir()),
      createRecordOrdering(2, 4),
      Vector.empty,
      numOfPar = 3,
      internalSortWorkerId = 5,
      internalSorterOutputDirectory = outDir
    )

    val files = sorter.madeFile(0)
    assertEquals(files.size, 3)
    assert(files.head.getFileName.toString.contains("file_5_1_0.dat"))
  }

  // -------------------------
  // TEST 3: partition 테스트
  // -------------------------
  test("partition should divide records according to pivots") {

    val pivot1 = record(10)
    val pivot2 = record(20)

    val sorter = new TestSorter(
      Seq(tempDir()),
      createRecordOrdering(1, 1),
      Vector(pivot1, pivot2),
      numOfPar = 3,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = tempDir()
    )

    val input = Seq(
      record(5),   // → partition 0
      record(10),  // → partition 0 (<= pivot1)
      record(15),  // → partition 1 (<= pivot2)
      record(30)   // → partition 2
    )

    val result = sorter.partition(input)
    assertEquals(result.map(_.size), Seq(2,1,1))
  }

  // -------------------------
  // TEST 4: saveFile 테스트 (실제 파일에 기록)
  // -------------------------
  test("saveFile should write records to output paths") {
    val outDir = tempDir()

    val sorter = new TestSorter(
      Seq(tempDir()),
      createRecordOrdering(1, 1),
      Vector.empty,
      numOfPar = 2,
      internalSortWorkerId = 0,
      internalSorterOutputDirectory = outDir
    )

    val paths = sorter.madeFile(0)
    val input = Seq(
      Seq(record(1), record(2)),
      Seq(record(3))
    )

    sorter.saveFile(input, paths)

    assert(Files.exists(paths(0)))
    assert(Files.exists(paths(1)))
  }

  // -------------------------
  // TEST 5: runSortAndPartition 통합 테스트
  // -------------------------
  test("runSortAndPartition should sort, partition, and save files") {
    val inputDir = tempDir()
    val outDir = tempDir()

    // block_0.dat 생성 (테스트용)
    val block0 = inputDir.resolve("block_0.dat")
    Files.write(block0, Array[Byte](30, 10, 20)) // 3 records, each 1 byte

    val sorter = new TestSorter(
      Seq(inputDir),
      createRecordOrdering(1, 1),
      Vector(record(15), record(25)),
      numOfPar = 3,
      internalSortWorkerId = 1,
      internalSorterOutputDirectory = outDir
    )

    val resultPaths = sorter.runSortAndPartition()

    assertEquals(resultPaths.size, 3)
    resultPaths.foreach(p => assert(Files.exists(p)))
  }
}
