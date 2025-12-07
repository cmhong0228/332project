package distributedsorting.logic

import java.nio.file.{Files, Path}
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.Duration
import java.util.concurrent.Executors
import com.typesafe.scalalogging.LazyLogging

import distributedsorting.distributedsorting.{Record, Key}

// --- Core Trait ---

trait InternalSorter extends LazyLogging {
  /**
   * input directory들의 Seq.
   */
  val internalSorterDirectories: Seq[Path]

  val ordering : Ordering[Record]

  val filePivot : Vector[Record]


  val numOfPar : Int

  val internalSortWorkerId : Int

  val internalSorterOutputDirectory : Path

  val INTERNAL_SORT_USABLE_MEMORY_RATIO: Double
  val numCores = Runtime.getRuntime.availableProcessors()
  val maxHeapSize = Runtime.getRuntime.maxMemory()
  lazy val safeMemoryLimit = (maxHeapSize * INTERNAL_SORT_USABLE_MEMORY_RATIO).toLong
  lazy val maxFileSize: Long = filePath.map { x =>
    Files.size(x)
  }.max
  lazy val memoryBasedThreadLimit = if (maxFileSize > 0) {
    (safeMemoryLimit / maxFileSize).toInt
  } else {
    numCores
  }
  lazy val optimalThreadCount = math.max(1, math.min(numCores, memoryBasedThreadLimit))


  // --- 유틸리티 함수 정의 ---
  /**
   * 주어지는 internalSorterDirectories에 있는 파일들의 경로를 리스트에 저장
   * @return inputDirectory의 파일들의 경로를 저장한 List[Path]를 반환
   */
  def madeFilePath(): List[Path] = {
    internalSorterDirectories
      .filter(dir => Files.exists(dir) && Files.isDirectory(dir))
      .flatMap { dir =>
        val stream = Files.list(dir)
        try {
          stream.iterator().asScala
            .filter(path => Files.isRegularFile(path))
            .toList
        } finally {
          stream.close()
        }
      }
      .toList
  }

  /**
   * 파티션 개수 만큼의 Path 생성
   * 각 파일들의 이름은 file_i_j_k.dat (i: InternalSorterWorkerId, j: 각 partition index(1~numOfPar), k: File index)
   * @param k - 각 파티션에 할당될 파일의 index
   * @return 생성된 파일 경로 List[Path]를 반환
   */
  def madeFile(k: Int): List[Path] = {
    // TODO: numOfPar, internalSortWorkerId, filePiece 등의 필드를 사용하여
    //       요구되는 파일명 패턴(file_i_j_k.dat)에 맞는 경로를 생성하는 로직 구현
    val paths = for {
      j <- 1 to numOfPar // j: Partition Index (1-based)
    } yield {
      val i = internalSortWorkerId // i: Worker ID
      // 경로 패턴: file_i_j_k.dat
      internalSorterOutputDirectory.resolve(s"file_${i}_${j}_${k}.dat")
    }
    paths.toList
  }


  // Used lazy val to avoid initialization order dependency on InternalSorterDirectories
  // madeFilePath 호출 시 인자 없음 (트레이트 필드 사용)
  lazy val filePath: List[Path] = madeFilePath().sorted


  lazy val filePiece: Int = filePath.size


  // =======================================================
  // 배치(Batch) 기반 파티션/저장 함수
  // =======================================================

  /**
   * TODO
   * partition(각 데이터를 pivot의 범위와 순차적으로 비교하면서 이에 해당하는 값을 시퀸스에 저장)
   * pivot에 따라서 시퀸스를 나누어 저장(numOfPar 길이의 시퀸스)
   * 범위가 없는 경우에는 빈 시퀸스로 둠
   * @param sortedRecords - sorting 단계를 거쳐서 정렬된 Record
   * @return Record들을 pivot의 범위에 따라 분류한 Seq[Seq[Record]] (배치 결과)
   */
  def partition(sortedRecords: Seq[Record]): Seq[Seq[Record]] = {
    // TODO: sortedRecords를 filePivot과 ordering을 사용하여 numOfPar 크기의 시퀀스로 분할하는 로직 구현

    val partitions = Array.fill(numOfPar)(ArrayBuffer.empty[Record])

    sortedRecords.foreach { record =>
      // pivot보다 작은 값은 이전 partition, pivot 이상이면 해당 partition
      val index = filePivot.indexWhere(pivot => ordering.compare(record, pivot) < 0)
      val partitionIndex = if (index == -1) numOfPar - 1 else index
      partitions(partitionIndex) += record
    }

    partitions.map(_.toSeq).toSeq
  }

  /**
   * TODO
   * partition 후 반환된 Seq[Seq[Record]]를 각 해당하는 파일에 저장
   * output directory에 저장하는 부분은 RecordWriter 활용
   * Partition 증가 index 그대로
   * @param partitionResult - partition의 반환값 (분류된 레코드 시퀀스)
   * @param outputPath - 각 File들을 저장할 output directory의 경로
   */
  def saveFile(partitionResult : Seq[Seq[Record]], outputPath : List[Path]): Unit = {
    // TODO: partitionResult의 각 시퀀스를 RecordWriterRunner를 활용하여 해당 outputPath[i]에 저장하는 로직 구현
    partitionResult.zip(outputPath).foreach { case (records, path) =>
      Try {
        RecordWriterRunner.WriteRecordIterator(path, records.iterator)
      } match {
        case Failure(e) => throw new RuntimeException(s"FATAL: Failed to write partition to $path", e)
        case _ => // Success
      }
    }
  }


  /**
   * 주 실행 로직: 입력 파일을 정렬하고, 배치 함수를 호출합니다.
   */
  def runSortAndPartition(): List[Path] = {
    val localExecutor = Executors.newFixedThreadPool(optimalThreadCount)
    logger.info(s"[InternalSorter] sort&partition run, optimalThreadcount: $optimalThreadCount")
    implicit val localEc: ExecutionContext = ExecutionContext.fromExecutor(localExecutor)

    try {
      val futureFiles: Seq[Future[List[Path]]] = (0 until filePiece).map { x =>
        Future {
          val inputPath = filePath(x)
          
          val sortedRecords: List[Record] = {
            val fileIterator = new FileRecordIterator(inputPath)
            try {
              fileIterator.toList.sorted(ordering)
            } finally {
              fileIterator.close()
            }
          }

          val outputPaths: List[Path] = madeFile(x)
          val partitionResult = partition(sortedRecords)

          saveFile(partitionResult, outputPaths)

          outputPaths 
        }
      }

      val aggregatedFuture: Future[Seq[List[Path]]] = Future.sequence(futureFiles)

      val allFilesSeq: Seq[List[Path]] = Await.result(aggregatedFuture, Duration.Inf)
      
      allFilesSeq.flatten.toList

    } finally {
      localExecutor.shutdown()
      logger.debug("[InternalSorter] Thread Pool Shutdown.")
    }
  }
}