package distributedsorting.logic

import munit.FunSuite
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import distributedsorting.distributedsorting._

// --- WrappedRecord ---
case class WrappedRecord(bytes: Array[Byte])

// --- Test InternalSorter ---
class TestInternalSorter(
                          val internalSorterDirectories: Seq[Path],
                          val internalSorterOutputDirectory: Path,
                          val filePivot: Vector[WrappedRecord],
                          val internalSortWorkerId: Int,
                          val numOfPar: Int,
                          val ordering: Ordering[WrappedRecord]
                        ) {

  def madeFilePath(): List[Path] = {
    internalSorterDirectories.flatMap { dir =>
      if (Files.exists(dir) && Files.isDirectory(dir)) {
        Files.list(dir).iterator().asScala.filter(Files.isRegularFile(_)).toList
      } else Nil
    }.toList
  }

  def madeFile(k: Int): List[Path] = {
    (1 to numOfPar).map { j =>
      internalSorterOutputDirectory.resolve(s"file_${internalSortWorkerId}_${j}_$k.dat")
    }.toList
  }

  def filePiece: Int = madeFilePath().size

  def partition(sortedRecords: Seq[WrappedRecord]): Seq[Seq[WrappedRecord]] = {
    val partitions = Array.fill(numOfPar)(scala.collection.mutable.ArrayBuffer.empty[WrappedRecord])
    sortedRecords.foreach { record =>
      val idx = filePivot.indexWhere(pivot => ordering.compare(record, pivot) <= 0) match {
        case -1 => numOfPar - 1
        case i => i
      }
      partitions(idx) += record
    }
    partitions.map(_.toSeq).toSeq
  }

  def saveFile(partitionResult: Seq[Seq[WrappedRecord]], outputPath: List[Path]): Unit = {
    partitionResult.zip(outputPath).foreach { case (records, path) =>
      val bytesIterator: Iterator[Record] = records.iterator.map(_.bytes)
      RecordWriterRunner.WriteRecordIterator(path, bytesIterator)
    }
  }

  def runSortAndPartition(): List[Path] = {
    var allPartitionedFiles: List[Path] = List.empty
    val filePaths = madeFilePath()

    for ((inputPath, idx) <- filePaths.zipWithIndex) {
      val fileIterator = new FileRecordIterator(inputPath)
      try {
        // Array[Byte]를 즉시 WrappedRecord로 변환
        val records: List[WrappedRecord] = fileIterator.toList.map(WrappedRecord)
        // WrappedRecord 자체를 Ordering으로 정렬
        val sortedRecords = records.sorted(ordering)
        val outputPaths = madeFile(idx)
        val partitionResult = partition(sortedRecords)
        saveFile(partitionResult, outputPaths)
        allPartitionedFiles = outputPaths
      } finally fileIterator.close()
    }
    allPartitionedFiles
  }
}

class InternalSorterTests extends FunSuite {

  val recordLength = 8
  val keyLength = 4

  implicit val wrappedOrdering: Ordering[WrappedRecord] = new Ordering[WrappedRecord] {
    override def compare(a: WrappedRecord, b: WrappedRecord): Int = {
      (0 until keyLength).iterator
        .map(i => java.lang.Byte.toUnsignedInt(a.bytes(i)).compareTo(java.lang.Byte.toUnsignedInt(b.bytes(i))))
        .find(_ != 0).getOrElse(0)
    }
  }

  def makeWrapped(value: Byte): WrappedRecord = WrappedRecord(Array.fill(recordLength)(value))

  def withTempDir(testCode: Path => Any): Unit = {
    val dir = Files.createTempDirectory("internal-sorter-test")
    try testCode(dir)
    finally Files.walk(dir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
  }

  // 1. madeFilePath 테스트
  test("madeFilePath returns only files, not directories") {
    withTempDir { temp =>
      val inputDir = Files.createDirectory(temp.resolve("input"))
      Files.write(inputDir.resolve("block_0.dat"), Array[Byte](1))
      Files.createDirectory(inputDir.resolve("subdir"))

      val sorter = new TestInternalSorter(Seq(inputDir), temp, Vector.empty, 0, 1, wrappedOrdering)
      val paths = sorter.madeFilePath()

      assertEquals(paths.size, 1)
      assertEquals(paths.head.getFileName.toString, "block_0.dat")
    }
  }

  // 2. madeFile 테스트
  test("madeFile generates correct paths based on workerId and k") {
    withTempDir { temp =>
      val sorter = new TestInternalSorter(Nil, temp, Vector.empty, internalSortWorkerId = 3, numOfPar = 2, wrappedOrdering)
      val res = sorter.madeFile(5)

      assertEquals(res.size, 2)
      assertEquals(res.head.getFileName.toString, "file_3_1_5.dat")
      assertEquals(res(1).getFileName.toString, "file_3_2_5.dat")
    }
  }

  // 3. partition 테스트
  test("partition divides records by pivot correctly") {
    val pivots = Vector(makeWrapped(5), makeWrapped(10))
    val sorter = new TestInternalSorter(Nil, Paths.get("/tmp"), pivots, 0, pivots.size, wrappedOrdering)

    val input = Seq(makeWrapped(1), makeWrapped(7), makeWrapped(12))
    val res = sorter.partition(input)

    assertEquals(res.size, pivots.size)
    assertEquals(res(0).map(_.bytes.head), Seq(1.toByte))
    assertEquals(res(1).map(_.bytes.head), Seq(7.toByte, 12.toByte))
  }

  // 4. saveFile 테스트
  test("saveFile writes correct bytes to file") {
    withTempDir { temp =>
      val sorter = new TestInternalSorter(Nil, temp, Vector.empty, 0, 2, wrappedOrdering)

      val part = Seq(Seq(makeWrapped(1.toByte)), Seq(makeWrapped(2.toByte)))
      val outPaths = sorter.madeFile(0)

      sorter.saveFile(part, outPaths)

      val firstBytes = Files.readAllBytes(outPaths.head)
      val secondBytes = Files.readAllBytes(outPaths(1))

      assertEquals(firstBytes.head, 1.toByte)
      assertEquals(secondBytes.head, 2.toByte)
    }
  }

  // 5. runSortAndPartition 통합 테스트
  test("runSortAndPartition fully processes input files and creates partitioned output") {
    withTempDir { temp =>
      val inputDir = Files.createDirectory(temp.resolve("input"))
      val outputDir = Files.createDirectory(temp.resolve("output"))

      // input 파일 생성
      val inputPath = inputDir.resolve("block_0.dat")
      Files.write(inputPath, Array[Byte](3, 1, 2, 4, 5, 6, 7, 8))

      val pivots = Vector(makeWrapped(2.toByte), makeWrapped(6.toByte))
      val sorter = new TestInternalSorter(Seq(inputDir), outputDir, pivots, 0, pivots.size, wrappedOrdering)

      val result = sorter.runSortAndPartition()

      assertEquals(result.size, pivots.size)
      result.foreach(path => assert(Files.exists(path)))
    }
  }

}
