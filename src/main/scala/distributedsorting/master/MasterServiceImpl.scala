package distributedsorting.master

import distributedsorting.distributedsorting._
import distributedsorting.logic._
import scala.concurrent.{Future, Promise, ExecutionContext}
import com.typesafe.config.ConfigFactory
import distributedsorting.distributedsorting._
import java.util.concurrent.{ConcurrentLinkedQueue, CopyOnWriteArrayList}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.jdk.CollectionConverters._
import com.google.protobuf.ByteString

// master를 종료 시키기 위한 trait
trait ShutdownController {
    def initiateShutdown(): Unit
    def signalShutdown(): Unit
}

class MasterServiceImpl(val numWorkers: Int, private val shutdownController: ShutdownController)(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService with SamplingPolicy with PivotSelector{
    private val config = ConfigFactory.load()
    private val configPath = "distributedsorting"

    // ==================================
    // 상태 관리 변수
    // ==================================
    private val connectedWorkersCount = new AtomicInteger(0)

    case class WorkerKey(ip: String, paths: Set[String])

    private val workerMap = scala.collection.mutable.Map[WorkerKey, WorkerInfo]()

    private val workers = new Array[WorkerInfo](numWorkers)

    private var isCollectedAllWorkers = false
    
    // ==================================
    // Registration
    // ==================================
    private val pendingRegisterPromises = new CopyOnWriteArrayList[Promise[RegisterResponse]]()

    private var totalInputRecords: Long = 0

    /**
     * [RPC 메서드] Worker가 Master에 등록할 때 사용
     * @param request RegisterRequest worker 정보 포함
     * @return RegisterResponse 
     */
    override def registerWorker(request: WorkerRegisterInfo): Future[RegisterResponse] = {
        val myPromise = Promise[RegisterResponse]()
        val workerInputpaths = request.paths.toSet
        val workerKey = new WorkerKey(request.ip, workerInputpaths)

        this.synchronized {
            var currentCount = 0
            if (workerMap.contains(workerKey)) { // 재 접속한 경우
                currentCount = connectedWorkersCount.get()
                val priorWorkerInfo = workerMap(workerKey)
                val workerId = priorWorkerInfo.workerId
                val index = workerId - 1
                assert(workers(index).workerId == workerId)
                val workerInfo = new WorkerInfo(workerId, request.ip, request.port)
                workers(index) = workerInfo
                pendingRegisterPromises.add(myPromise)
                workerMap(workerKey) = workerInfo
                finishedWorkers(index) = false // 재 접속 했기 때문에 초기화
            } else { // 새로운 접속 
                currentCount = connectedWorkersCount.incrementAndGet()
                val workerInfo = new WorkerInfo(currentCount, request.ip, request.port)
                workers(currentCount-1) = workerInfo
                workerMap(workerKey) = workerInfo
                pendingRegisterPromises.add(myPromise)
                totalInputRecords = totalInputRecords + request.numRecords
            }  
            
            if (currentCount >= numWorkers) {
                val allWorkersSeq = workers.toSeq

                val response = RegisterResponse(
                    success = true,
                    workerList = allWorkersSeq
                )

                pendingRegisterPromises.asScala.foreach { promise =>
                    promise.success(response)
                }
                
                pendingRegisterPromises.clear()

                if (!isCollectedAllWorkers) {
                    val workerIps = allWorkersSeq.map(_.ip).mkString(", ")
                    println(workerIps)
                    isCollectedAllWorkers = true
                }                
            }
        }
        myPromise.future
    }

    // ==================================
    // termination
    // ==================================
    private val pendingTerminationPromises = new CopyOnWriteArrayList[Promise[CompletionResponse]]()
    private val finishedWorkers = new Array[Boolean](numWorkers)

    private val finalRecordsForEachWorkers = new Array[Long](numWorkers)

    override def reportCompletion(request: CompletionRequest): Future[CompletionResponse] = {
        val myPromise = Promise[CompletionResponse]() 
        val workerId = request.getWorkerInfo.workerId
        val index = workerId - 1

        this.synchronized {
            pendingTerminationPromises.add(myPromise)
            if (!finishedWorkers(index)) {
                finishedWorkers(index) = true
                finalRecordsForEachWorkers(index) = request.numRecords
            }            
            
            if (finishedWorkers.forall(_ == true)) {
                assert(totalInputRecords == finalRecordsForEachWorkers.sum)
                val response = CompletionResponse(success = true)

                pendingTerminationPromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingTerminationPromises.clear()

                shutdownController.signalShutdown()
            }
        }
        
        myPromise.future
    }
    
    // ==================================
    // communication
    // ==================================
    private val pendingSortTerminationPromises = new CopyOnWriteArrayList[Promise[FileIdMap]]()
    private var fileIdsResponses: Seq[Seq[FileIdMessage]] = List()
    private val reportedFileIdWorkers = new Array[Boolean](numWorkers)

    override def reportFileIds(request: FileIdRequest): Future[FileIdMap] = {
        val myPromise = Promise[FileIdMap]()
        val workerId = request.workerId
        val index = workerId - 1

        this.synchronized {
            pendingSortTerminationPromises.add(myPromise)
            if (reportedFileIdWorkers(index) == false) {
                reportedFileIdWorkers(index) = true
                fileIdsResponses = fileIdsResponses :+ request.fileIds
            }

            if (reportedFileIdWorkers.forall(_ == true)) {
                val map = fileIdsResponses.flatten.groupBy(_.j).map { case (key, list) =>
                    key -> new FileIdList(list)
                }
                val response = new FileIdMap(map)

                pendingSortTerminationPromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingSortTerminationPromises.clear()
            }
        }
        myPromise.future
    }

    override def requestWorkerInfos(request: WorkerInfosRequest): Future[WorkerInfosReply] = {
        this.synchronized {
            val currentWorkers = workers.toSeq

            val response = WorkerInfosReply(
                workerList = currentWorkers
            )

            Future.successful(response)
        }
    }

    // ==================================
    // Sampling
    // ==================================
    /*
     * Sampling(Master)
     * 역할
     * * worker로 부터 Record 개수를 받아 sampling ratio를 계산 후 전송
     * * worker로 부터 Sampling 된 records를 받아 pivots을 구한 후 전송
     */
     
    // key의 길이
    val KEY_SIZE: Int = config.getInt(s"$configPath.record-info.key-length")
    // record의 길이
    val RECORD_SIZE: Int = config.getInt(s"$configPath.record-info.record-length")
    // memory의 크기
    val MEMORY_SIZE: Long = config.getBytes(s"$configPath.cluster-info.node-info.memory").toLong
    // 최대 사용할 메모리의 비율
    val MAX_MEMORY_USAGE_RATIO: Double = config.getDouble(s"$configPath.sampling-control.max-memory-usage-ratio")
    // 머신 당 받고자 하는 평균적인 바이트 수
    val BYTES_PER_MACHINE: Long = config.getBytes(s"$configPath.sampling-control.bytes-per-machine").toLong
    // 머신 마다 넘지 않도록 설계한 바이트 수
    //val MAX_BYTES_PER_MACHINE_DESIGNED: Long = ???
    // 정렬 시 사용할 ordering
    val useSimpleSampling = config.getBoolean(s"$configPath.sampling-control.use-simple-sampling")
    val ordering: Ordering[Key] = {
        if (useSimpleSampling) {
            createRecordOrdering(KEY_SIZE, RECORD_SIZE)
        } else {
            createRecordOrdering(KEY_SIZE, KEY_SIZE)
        }
    }

    private val totalRecordSum = new AtomicLong(0)
    private val pendingRecordCountPromises = new CopyOnWriteArrayList[Promise[SamplingRatio]]()
    private val pendingSamplePromises = new CopyOnWriteArrayList[Promise[SampleDecision]]()

    private val allSamples = new ConcurrentLinkedQueue[Key]()

    private val recordCountedWorkers = new Array[Boolean](numWorkers)
    private val sampledWorkers = new Array[Boolean](numWorkers)
    private var isCompletedPivots = false
    private var pivots: Vector[Record] = _

    /**
     * [RPC 메서드] Worker가 자신이 가진 레코드 개수를 Master에게 보고할 때 호출
     * @param request RecordCountReport (Worker ID 및 해당 Worker의 총 레코드 개수 포함)
     * @return SamplingRatio (계산된 샘플링 확률)을 Worker에게 전송
     * 내부적으로 SamplingPolicy.calculateSamplingRatio를 호출
     */
    override def reportRecordCount(request: RecordCountReport): Future[SamplingRatio] = {
        val myPromise = Promise[SamplingRatio]()
        val workerId = request.workerId
        val index = workerId - 1

        this.synchronized {
            pendingRecordCountPromises.add(myPromise)
            if (!recordCountedWorkers(index)){
                totalRecordSum.addAndGet(request.totalRecordCount)
                recordCountedWorkers(index) = true
            }

            if (recordCountedWorkers.forall(_ == true)) {
                val totalRecords = totalRecordSum.get()
                val ratio = calculateSamplingRatio(totalRecords)
                
                val response = SamplingRatio(ratio = ratio, isFinished = isCompletedPivots)
                
                pendingRecordCountPromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingRecordCountPromises.clear()
            }
        }
        myPromise.future
    }

    /**
     * [RPC 메서드] Worker가 로컬에서 샘플링한 Key 리스트를 Master에게 보낼 때 호출
     * * Master는 모든 Worker의 샘플을 통합한 후, PivotSelector의 SortSamples와 selectPivots을 호출하여
     * 파티셔닝 경계 키를 결정
     * @param request SampleKeyList (Worker ID 및 샘플링된 Key 리스트 포함)
     * @return SampleDecision (계산된 최종 Pivot Key 리스트를 Worker에게 전송)
     */
    override def sendSampleKeys(request: SampleKeyList): Future[SampleDecision] = {
        val myPromise = Promise[SampleDecision]()
        val workerId = request.workerId
        val index = workerId - 1

        this.synchronized {
            pendingSamplePromises.add(myPromise)
        
            if (!sampledWorkers(index)) {
                val keysFromWorker: Seq[Key] = request.keys.map(_.value.toByteArray)
                allSamples.addAll(keysFromWorker.asJava)            
                sampledWorkers(index) = true
            }
            
            if (sampledWorkers.forall(_ ==  true)) {
                if (!isCompletedPivots) {
                    val aggregatedKeys: Seq[Key] = allSamples.asScala.toSeq
                    val sortedKeys = sortSamples(aggregatedKeys)
                    val pivotKeys: Vector[Key] = selectPivots(sortedKeys)
                    pivots = createPaddedPivots(pivotKeys)   
                    isCompletedPivots = true     
                }
                
                val protoPivots: Seq[KeyMessage] = pivots.map { keyArray =>
                    KeyMessage(value = ByteString.copyFrom(keyArray))
                }
                val partitionInfo = PartitionInfo(pivots = protoPivots)
                val response = SampleDecision(
                    proceed = true,
                    content = SampleDecision.Content.PartitionInfo(partitionInfo)
                )
                
                pendingSamplePromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingSamplePromises.clear()
            }
        }
        myPromise.future
    }

    /**
     * [RPC 메서드] Worker가 로컬에서 샘플링한 Key 리스트를 Master에게 보낼 때 호출
     * * Master는 모든 Worker의 샘플을 통합한 후, PivotSelector의 SortSamples와 selectPivots을 호출하여
     * 파티셔닝 경계 키를 결정
     * @param request SampleKeyList (Worker ID 및 샘플링된 Key 리스트 포함)
     * @return SampleDecision (계산된 최종 Pivot Key 리스트를 Worker에게 전송)
     */
    override def sendSimpleSampleKeys(request: SampleKeyList): Future[SampleDecision] = {
        val myPromise = Promise[SampleDecision]()
        val workerId = request.workerId
        val index = workerId - 1

        this.synchronized {
            pendingSamplePromises.add(myPromise)
        
            if (!sampledWorkers(index)) {
                val keysFromWorker: Seq[Record] = request.keys.map(_.value.toByteArray)
                allSamples.addAll(keysFromWorker.asJava)            
                sampledWorkers(index) = true
            }
            
            if (sampledWorkers.forall(_ ==  true)) {
                if (!isCompletedPivots) {
                    val aggregatedKeys: Seq[Record] = allSamples.asScala.toSeq
                    val sortedKeys = sortSamples(aggregatedKeys)
                    val pivotKeys: Vector[Record] = selectPivots(sortedKeys)
                    pivots = pivotKeys
                    isCompletedPivots = true     
                }
                
                val protoPivots: Seq[KeyMessage] = pivots.map { keyArray =>
                    KeyMessage(value = ByteString.copyFrom(keyArray))
                }
                val partitionInfo = PartitionInfo(pivots = protoPivots)
                val response = SampleDecision(
                    proceed = true,
                    content = SampleDecision.Content.PartitionInfo(partitionInfo)
                )
                
                pendingSamplePromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingSamplePromises.clear()
            }
        }
        myPromise.future
    }

}