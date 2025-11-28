package distributedsorting.logic

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path}
import java.io.{File, IOException}
import java.util.Comparator
import scala.jdk.CollectionConverters._
import scala.collection.mutable.PriorityQueue
import java.util.concurrent.Executors
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.Duration
import java.lang.management.ManagementFactory
import com.sun.management.UnixOperatingSystemMXBean
import scala.util.Try

/**
 * 대용량 데이터를 처리하기 위한 외부 정렬(External Sorting) 기능을 정의하는 trait
 * 메모리에 한 번에 올릴 수 없는 대규모 파일 여러개를 작은 단위로 분할하여 정렬하고,
 * 이를 효율적으로 병합(k-way merge)하는 기능을 제공
 * 
 * ==============================
 * === 사용법 ===
 * ==============================
 * * 초기 setting
 * * * externalSorterInputDirectory, externalSorterOutputDirectory, externalSorterTempDirectory 경로 지정
 * * * externalSorterOrdering ordering 지정
 * * executeExternalSort 호출(파일 읽기, merge, 파일 정리 모두 수행됨)
 */
trait ExternalSorter {
    val RECORD_SIZE: Int

    /**
     * 정렬 작업을 위해 레코드를 읽어올 입력 디렉토리의 경로
     * 이 디렉토리 내의 모든 파일이 정렬 대상
     */
    val externalSorterInputDirectory: Path

    /**
     * 최종적으로 정렬된 결과를 저장할 출력 디렉토리의 경로
     * 정렬된 단일 파일이 이 위치에 저장
     */
    val externalSorterOutputDirectory: Path

    /**
     * 정렬 과정 중 생성되는 임시 파일을 저장할 경로
     * 작업 완료 후 임시 파일들은 정리되어야 함
     */
    val externalSorterTempDirectory: Path

    /**
     * 레코드(Record) 간의 순서를 결정하는 데 사용되는 Ordering 인스턴스
     * 이 인스턴스는 레코드의 비교 로직을 정의
     */
    val externalSorterOrdering: Ordering[Record]

    /**
     * 결과를 저장할 각 파일의 크기
     */
    val chunkSize: Long

    /**
     * 결과를 저장할 파일 이름의 prefix
     */
    val outputPrefix: String

    /**
     * 결과를 저장할 파일 이름의 postfix의 시작값
     */
    val outputStartPostfix: Int
    
    val filePivot : Vector[Record]
    val externalSorterWorkerId: Int

    /** 
     * config for numMaxMergeGroup
     */
    val MEMORY_SIZE: Long
    val EXTERNAL_SORT_USABLE_MEMORY_RATIO: Double
    val BUFFER_SIZE: Long
    val BASIC_MAX_MERGE_FILES: Int
    val MAX_FILES_RATIO: Double

    
    val maxMemory = Runtime.getRuntime.maxMemory()
    lazy val safeMemory = (maxMemory * EXTERNAL_SORT_USABLE_MEMORY_RATIO).toLong

    lazy val totalAvailableBuffers: Int = (safeMemory / BUFFER_SIZE).toInt

    lazy val maxFiles = (getMaxOpenFilesLimit() * MAX_FILES_RATIO).toInt

    /**
     * k-way merge 단계에서 한 번에 병합할 수 있는 최대 파일 또는 스트림의 개수(k 값)
     * 이 값은 시스템의 메모리 제한을 고려하여 설정
     * 총 버퍼 수 - 출력 버퍼(1)
     */
    lazy val numMaxMergeGroup: Int = math.min(totalAvailableBuffers, maxFiles) - 1

    /**
     * 주어진 파일 경로 시퀀스를 최대 `numMaxMergeGroup` 크기의 그룹으로 분할
     * 이는 병합 작업을 여러 단계로 나누어 수행할 때 사용
     *
     * @param fileSeq 분할할 파일들의 `Path` 시퀀스
     * @return 최대 k개씩 묶인 `Path` 시퀀스의 시퀀스
     */
    def splitGroup(fileSeq: Seq[Path]): Seq[Seq[Path]] = {
        fileSeq.grouped(numMaxMergeGroup).toSeq
    }

    /**
     * 정렬된 레코드를 담고 있는 여러 개의 Iterator를 하나의 정렬된 Iterator로 병합
     * 이 메소드는 k-way merge 알고리즘을 구현
     *
     * @param recordIters 병합할 정렬된 레코드 Iterator들의 시퀀스
     * @return 모든 입력 레코드를 정렬된 순서로 포함하는 단일 Iterator
     */
    def iteratorMerge(recordIters: Seq[Iterator[Record]]): Iterator[Record] = {
        implicit val pqOrdering: Ordering[(Record, Iterator[Record])] = 
            Ordering.by((_: (Record, Iterator[Record]))._1)(externalSorterOrdering.reverse)

        val pq = new PriorityQueue[(Record, Iterator[Record])]()

        recordIters.foreach { iter =>
            if (iter.hasNext) {
                val nextVal = iter.next()
                assert(externalSorterWorkerId <= 1 || externalSorterOrdering.compare(nextVal, filePivot(externalSorterWorkerId-2)) >= 0)
                assert(externalSorterWorkerId > filePivot.size || externalSorterOrdering.compare(nextVal, filePivot(externalSorterWorkerId-1)) < 0)
                pq.enqueue((nextVal, iter))
            }
        }

        // (4) 큐가 빌 때까지 레코드를 뽑아내는 새 이터레이터 반환
        new Iterator[Record] {
            def hasNext: Boolean = pq.nonEmpty

            def next(): Record = {
                // (5) 가장 작은 레코드(minRecord)와 해당 레코드의 원본(sourceIter)을 큐에서 추출
                val (minRecord, sourceIter) = pq.dequeue()

                // (6) 해당 원본(sourceIter)에 레코드가 더 남아있다면,
                //     다음 레코드를 뽑아서 다시 큐에 추가
                if (sourceIter.hasNext) {
                    pq.enqueue((sourceIter.next(), sourceIter))
                }

                // (7) 가장 작은 레코드 반환
                minRecord
            }
        }
    }

    /**
     * 파일 경로 시퀀스를 받아서, 해당 파일들의 내용을 읽고 k-way merge를 수행하여
     * 최종적으로 정렬된 레코드 Iterator를 반환
     *
     * @param fileSeq 내용을 병합하고 정렬할 파일들의 `Path` 시퀀스
     * @return 병합된 결과를 순차적으로 제공하는 `RecordIterator`
     */
    def merge(fileSeq: Seq[Path]): Path = {
        require(fileSeq.nonEmpty)
        val totalFiles = fileSeq.size
        
        val maxSingleK = numMaxMergeGroup

        val (optimalK, threadCount) = if (totalFiles <= maxSingleK) {
            println(s"[ExternalSorter] Single Pass Merge, maxSingleK = $maxSingleK (maxFiles: $maxFiles)")
            (totalFiles, 1)
        } else {
            val threads = Runtime.getRuntime.availableProcessors()
            
            val memPerThread = safeMemory / threads
            
            val parallelK = math.max(2, math.min((memPerThread / BUFFER_SIZE).toInt, (maxFiles / threads).toInt) - 1)
            println(s"[ExternalSorter] Multi Pass Merge, k = $parallelK")
            
            (parallelK, threads)
        }

        val executor = Executors.newFixedThreadPool(threadCount)
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

        var currentFiles = fileSeq
        var pass = 0

        try {
            while (currentFiles.size > 1) {
                pass += 1
                val groups: Seq[Seq[Path]] = currentFiles.grouped(optimalK).toSeq

                val futures = groups.map { group =>
                    Future {
                        val tempOutPath = externalSorterTempDirectory.resolve(
                            s"pass-$pass-group-${group.head.hashCode}-${System.nanoTime()}.bin"
                        )

                        var iterators: Seq[FileRecordIterator] = null
                        try {
                            iterators = group.map(path => new FileRecordIterator(path))

                            val mergedIter = iteratorMerge(iterators)

                            RecordWriterRunner.WriteRecordIterator(tempOutPath, mergedIter)

                            tempOutPath
                        } finally {
                            if (iterators != null) iterators.foreach(_.close())
                        }
                    }
                }
                currentFiles = Await.result(Future.sequence(futures), Duration.Inf)
            }
            
            currentFiles.head
            
        } finally {
            executor.shutdown()
        }
    }

    def getMaxOpenFilesLimit(): Long = {
        val osBean = ManagementFactory.getOperatingSystemMXBean
        
        val isWindows = System.getProperty("os.name").toLowerCase.contains("win")

        if (isWindows) {
            BASIC_MAX_MERGE_FILES
        } else {
            osBean match {
                case unixBean: UnixOperatingSystemMXBean =>
                    unixBean.getMaxFileDescriptorCount
                case _ =>
                    BASIC_MAX_MERGE_FILES
            }
        }
    }

    /**
     * 입력 디렉토리에서 정렬 대상이 되는 모든 일반 파일의 Path 시퀀스를 가져옴
     * @return 정렬 대상 파일들의 Path 시퀀스
     */
    def getInputFiles(): Seq[Path] = {
        if (!Files.isDirectory(externalSorterInputDirectory)) {
            return Seq.empty
        }

        // Java Stream을 Scala Seq로 변환 (try-with-resources)
        val stream = Files.walk(externalSorterInputDirectory, 1) // 1단계 깊이만 탐색
        try {
            stream.iterator().asScala
                .filter(path => path != externalSorterInputDirectory) // 디렉터리 자신 제외
                .filter(Files.isRegularFile(_)) // 파일만 필터링
                .toSeq
        } finally {
            stream.close() // 스트림 리소스 해제
        }
    }

    /**
     * 임시 디렉토리에 생성된 모든 임시 파일 및 디렉토리를 정리
     * 이 메소드는 작업의 성공/실패 여부와 관계없이 실행
     */
    def cleanUpTempFiles(): Unit = {
        val dir = externalSorterTempDirectory.toFile
        if (dir.exists()) {
            
            def deleteRecursively(file: File): Unit = {
                if (file.isDirectory) {
                    file.listFiles().foreach(deleteRecursively)
                }
                file.delete()
            }
            
            dir.listFiles().foreach(deleteRecursively)
        }
    }

    /**
     * 주어진 파일을 여러개의 파일로 나누어 저장
     * 파일 이름은 prefix.num 형식
     * @param inputFilePath split할 파일의 경로
     * @param outputDirectory 파일을 저장할 위치
     * @param recordsPerFile 파일 당 포함할 레코드 수 
     * @param filePrefix 파일 이름 prefix
     * @param startPostfixNumber 파일 이름 postfix
     */
    def splitFile(
        inputFilePath: Path,
        outputDirectory: Path,
        recordsPerFile: Long,
        filePrefix: String,
        startPostfixNumber: Int
    ): Unit = {        
        if (!Files.exists(inputFilePath) || recordsPerFile <= 0 || Files.size(inputFilePath) == 0) {
            return
        }

        val inputIter = new FileRecordIterator(inputFilePath, recordSize = RECORD_SIZE)
        var fileCounter = startPostfixNumber
        assert(inputIter.hasNext)
        try {
            // (1) 입력 이터레이터가 끝날 때까지 반복
            while (inputIter.hasNext) {
                // (2) recordsPerFile 개수만큼만 레코드를 가져오는 "Sub-Iterator" 생성
                val currentChunkIter = inputIter.take(recordsPerFile.toInt) // (take는 Int만 받음)
                
                // (3) 출력 파일 경로 생성 (e.g., "sorted.001")
                val outPath = outputDirectory.resolve(s"$filePrefix.$fileCounter")

                // (4) Sub-Iterator의 내용을 파일에 씀
                // (currentChunkIter가 끝나면 WriteRecordIterator가 종료됨)
                RecordWriterRunner.WriteRecordIterator(outPath, currentChunkIter)
                
                fileCounter += 1
            }
        } finally {
            inputIter.close() // (5) 원본 입력 이터레이터 닫기
        }
    }

    /**
     * 외부 정렬의 전체 과정을 실행하는 메인 메소드
     * 파일 목록 읽기, 정렬 수행, 임시 파일 정리까지 모두 담당
     */
    def executeExternalSort(): Unit = {
        require(RECORD_SIZE > 0, "RECORD_SIZE must be positive")
        require(chunkSize > 0, "chunkSize must be positive")

        try {
            // (1) 입력 파일(정렬된 런) 목록 가져오기
            val sortedRuns = getInputFiles()
            
            if (sortedRuns.isEmpty) {
                println("정렬할 입력 파일이 없습니다.")
                return
            }

            // (2) 다단계 병합 수행 -> 하나의 거대 정렬 파일(temp) 생성
            val finalMergedFile = merge(sortedRuns)
            assert(Files.size(finalMergedFile) > 0)

            // (3) 바이트 단위 chunkSize를 레코드 개수로 변환
            val recordsPerChunk = chunkSize / RECORD_SIZE
            if (recordsPerChunk == 0) {
                throw new IllegalArgumentException(s"chunkSize($chunkSize)가 RECORD_SIZE($RECORD_SIZE)보다 작습니다.")
            }

            // (4) 거대 정렬 파일을 최종 출력 디렉터리에 작은 청크로 분할
            splitFile(
                finalMergedFile,
                externalSorterOutputDirectory,
                recordsPerChunk,
                outputPrefix,
                outputStartPostfix
            )
            
        } catch {
            case e: Exception =>
                println(s"외부 정렬 중 오류 발생: ${e.getMessage}")
                throw e // 오류 재전파
        } finally {
            // (5) 성공/실패 여부와 관계없이 임시 파일 정리
            cleanUpTempFiles()
        }
    }
}