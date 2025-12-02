package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.logic._
import java.nio.file.Path
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Status, StatusRuntimeException}
import distributedsorting.distributedsorting.MasterServiceGrpc.MasterServiceStub
import com.google.protobuf.ByteString
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Try, Success, Failure}
import distributedsorting.worker.TestHelpers.FileStructure
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.LazyLogging

trait MasterClient extends RecordCountCalculator with RecordExtractor with Sampler with LazyLogging {
    implicit val ec: ExecutionContext
    
    val masterIp: String
    val masterPort: Int
    var workerInfo: WorkerInfo = _

    val workerInputPaths: Seq[String]
    val inputDirs: Seq[Path]
    val outputDir: Path
    val workerIp: String
    var workerPort: Int
    lazy val inputRecords = calculateTotalRecords(inputDirs)
    lazy val workerRegisterInfo: WorkerRegisterInfo = new WorkerRegisterInfo(ip = workerIp, port = workerPort, paths = workerInputPaths, numRecords = inputRecords)
    
    val simpleSamplingRecords: Int

    private lazy val channel: ManagedChannel = ManagedChannelBuilder.forAddress(masterIp, masterPort)
      .usePlaintext()
      .build()
    private lazy val masterClient: MasterServiceStub = MasterServiceGrpc.stub(channel)

    def cleanup(): Unit = {
        if (!channel.isShutdown) {
            channel.shutdownNow()
        }
    }

    private var _allWorkers: Seq[WorkerInfo] = Seq.empty[WorkerInfo]
    def getAllWorkers: Seq[WorkerInfo] = this.synchronized { _allWorkers }

    /**
     * [핵심] RPC 호출 재시도 헬퍼 함수
     * - 실패 시 1초 대기 후 무한 재시도
     * - 성공할 때까지 블로킹
     */
    private def callWithRetry[T](
    rpcCall: => Future[T], 
    operationName: String, 
    onMasterShutdown: Option[T] = None 
    ): T = {
        var result: Option[T] = None
        
        while (result.isEmpty) {
            try {
                val future = rpcCall 
                result = Some(Await.result(future, Duration.Inf)) 
            } catch {
                case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
                    if (onMasterShutdown.isDefined) {
                        logger.info(s"[Worker] $operationName: Master unavailable (likely shutdown). Using fallback value.")
                        return onMasterShutdown.get
                    }
                    
                    logger.info(s"[Worker] $operationName failed (Master unavailable). Retrying in 1s...")
                    Thread.sleep(1000)

                case NonFatal(e) =>
                    logger.info(s"[Worker] $operationName failed. Retrying in 1s... Error: ${e.getMessage}")
                    Thread.sleep(1000)
            }
        }
        result.get
    }

    // ======================= Registration =======================
    def registerWorker(): Unit = {
        // 복잡한 while 루프 대신 헬퍼 함수 사용
        val response = callWithRetry(
            masterClient.registerWorker(workerRegisterInfo),
            "RegisterWorker"
        )

        if (response.success) {
            this.synchronized {
                _allWorkers = response.workerList
            }
            val myWorkerInfo = _allWorkers.filter(w => w.ip == workerRegisterInfo.ip && w.port == workerRegisterInfo.port)
            assert(myWorkerInfo.length == 1, "Registered but cannot find self in worker list")
            workerInfo = myWorkerInfo.head
            logger.info(s"[Worker ${workerInfo.workerId}] Registered successfully.")
        } else {
            throw new RuntimeException("Registration failed logically (success=false)")
        }
    }
            
    // ======================= Termination =======================
    def reportCompletion(): Unit = {
        val completionReport = new CompletionRequest(Some(workerInfo), calculateTotalRecords(Seq(outputDir)))
        val fakeSuccessResponse = CompletionResponse(success = true)
        val response = callWithRetry(
            masterClient.reportCompletion(completionReport),
            "ReportCompletion",
            onMasterShutdown = Some(fakeSuccessResponse)
        )
        
        if (response.success) {
            logger.info(s"[Worker ${workerInfo.workerId}] Completion reported. Shutting down.")
            cleanup()
        }
    }

    // ======================= Communication =======================
    def reportFileIds(fileIdSet: Set[FileId]): FileStructure = {
        val fileIdMessageList = fileIdSet.map(fi => new FileIdMessage(fi.sourceWorkerId, fi.partitionId, fi.sortedRunId)).toList
        
        val response = callWithRetry(
            masterClient.reportFileIds(new FileIdRequest(workerInfo.workerId, fileIdMessageList)),
            "ReportFileIds"
        )

        val fileIdMap = response.fileIds.map { case (key, list) =>
            key -> list.fileIds.map(fi => new FileId(fi.i, fi.j, fi.k))
        }
        
        new FileStructure(fileIdMap)
    }

    def resetWorkerInfos(): Unit = {
        val request = new WorkerInfosRequest()

        val response = callWithRetry(
            masterClient.requestWorkerInfos(request),
            "RequestWorkerInfos"
        )

        this.synchronized {
            _allWorkers = response.workerList
        }
    }

    // ======================= Sampling =======================
    def executeSampling(inputDirs: Seq[Path]): Vector[Record] = {
        
        // --- 1단계: 레코드 수 보고 ---
        val localRecordCount = calculateTotalRecords(inputDirs)
        val report = RecordCountReport(
            workerId = workerInfo.workerId,
            totalRecordCount = localRecordCount
        )

        val samplingRatioMsg = callWithRetry(
            masterClient.reportRecordCount(report),
            "ReportRecordCount"
        )
        val ratio = samplingRatioMsg.ratio
        val isFinishedSampling = samplingRatioMsg.isFinished

        // --- 2단계: 샘플 전송 ---
        var localSamples: Seq[Key] = Seq.empty
        if (!isFinishedSampling) {
            localSamples = readAndExtractSamples(inputDirs, ratio)
        }

        val protoKeys: Seq[KeyMessage] = localSamples.map { keyArray =>
            KeyMessage(value = ByteString.copyFrom(keyArray))
        }

        val sampleList = SampleKeyList(
            workerId = workerInfo.workerId,
            keys = protoKeys
        )

        val decision = callWithRetry(
            masterClient.sendSampleKeys(sampleList),
            "SendSampleKeys"
        )

        decision.content match {
            case SampleDecision.Content.PartitionInfo(partitionInfo) =>
                val pivots: Vector[Record] = partitionInfo.pivots.map { keyMsg =>
                    keyMsg.value.toByteArray
                }.toVector
                pivots

            case SampleDecision.Content.Control(control) =>
                throw new RuntimeException(s"Re-sampling requested but not implemented.")
            
            case SampleDecision.Content.Empty =>
                throw new RuntimeException("Master returned empty decision.")
        }
    }

    def executeSimpleSampling(inputDirs: Seq[Path]): Vector[Record] = {
        val allFilePaths: Seq[Path] = getAllFilePaths(inputDirs)

        var numCurrentSamples = 0
        val collectedSamples = ListBuffer[Record]()
        for (filePath <- allFilePaths if numCurrentSamples < simpleSamplingRecords) {  
            val iterator = new FileRecordIterator(filePath)            
            try {
                while (iterator.hasNext && numCurrentSamples < simpleSamplingRecords) {
                val record = iterator.next()
                
                collectedSamples += record
                numCurrentSamples += 1
                }
            } finally {
                iterator.close()
            }
        }

        val protoKeys: Seq[KeyMessage] = collectedSamples.map { keyArray =>
            KeyMessage(value = ByteString.copyFrom(keyArray))
        }.toSeq

        val sampleList = SampleKeyList(
            workerId = workerInfo.workerId,
            keys = protoKeys
        )

        val decision = callWithRetry(
            masterClient.sendSimpleSampleKeys(sampleList),
            "SendSampleKeys"
        )

        decision.content match {
            case SampleDecision.Content.PartitionInfo(partitionInfo) =>
                val pivots: Vector[Record] = partitionInfo.pivots.map { keyMsg =>
                    keyMsg.value.toByteArray
                }.toVector
                pivots

            case SampleDecision.Content.Control(control) =>
                throw new RuntimeException(s"Re-sampling requested but not implemented.")
            
            case SampleDecision.Content.Empty =>
                throw new RuntimeException("Master returned empty decision.")
        }
    }

}