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
    // 상태 관리 변수 (Thread-Safe)
    // ==================================
    private val workers = new CopyOnWriteArrayList[WorkerInfo]()

    private val connectedWorkersCount = new AtomicInteger(0)
    
    // ==================================
    // Registration
    // ==================================
    private val pendingRegisterPromises = new CopyOnWriteArrayList[Promise[RegisterResponse]]()

    /**
     * [RPC 메서드] Worker가 Master에 등록할 때 사용
     * @param request RegisterRequest worker 정보 포함
     * @return RegisterResponse 
     */
    override def registerWorker(request: WorkerInfo): Future[RegisterResponse] = {
        val myPromise = Promise[RegisterResponse]()

        this.synchronized {
            workers.add(request)
            pendingRegisterPromises.add(myPromise)
            
            val currentCount = connectedWorkersCount.incrementAndGet()

            if (currentCount == numWorkers) {
                val allWorkersSeq = workers.asScala.toSeq

                val response = RegisterResponse(
                    success = true,
                    workerList = allWorkersSeq
                )

                pendingRegisterPromises.asScala.foreach { promise =>
                    promise.success(response)
                }
                
                pendingRegisterPromises.clear()

                val workerIps = allWorkersSeq.map(_.ip).mkString(", ")
                println(workerIps)
            }
        }
        myPromise.future
    }

    // ==================================
    // termination
    // ==================================
    private val finishedWorkersCount = new AtomicInteger(0)
    private val pendingTerminationPromises = new CopyOnWriteArrayList[Promise[CompletionResponse]]()

    override def reportCompletion(request: WorkerInfo): Future[CompletionResponse] = {
        val myPromise = Promise[CompletionResponse]() 

        this.synchronized {
            // TODO: 개수가 아닌 각 worker가 끝난건지 확인
            pendingTerminationPromises.add(myPromise)

            val currentFinished = finishedWorkersCount.incrementAndGet()
            
            if (currentFinished == numWorkers) {
                val response = CompletionResponse(success = true)

                pendingTerminationPromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingTerminationPromises.clear()

                //shutdownController.initiateShutdown()
                shutdownController.signalShutdown()
            }
        }
        
        myPromise.future
    }
    
    // ==================================
    // communication
    // ==================================
    private val finishedSortWorkersCount = new AtomicInteger(0)
    private val pendingSortTerminationPromises = new CopyOnWriteArrayList[Promise[CompletionResponse]]()

    override def reportSortCompletion(request: WorkerInfo): Future[CompletionResponse] = {
        val myPromise = Promise[CompletionResponse]() 

        this.synchronized {
            // TODO: 개수가 아닌 각 worker가 끝난건지 확인
            pendingSortTerminationPromises.add(myPromise)

            val currentFinished = finishedSortWorkersCount.incrementAndGet()
            
            if (currentFinished == numWorkers) {
                val response = CompletionResponse(success = true)

                pendingSortTerminationPromises.asScala.foreach { promise =>
                    promise.success(response)
                }

                pendingSortTerminationPromises.clear()
            }
        }
        
        myPromise.future
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
    val ordering: Ordering[Key] = createRecordOrdering(KEY_SIZE, KEY_SIZE)

    private val reportCounter = new AtomicInteger(0)
    private val totalRecordSum = new AtomicLong(0)
    private val samplingRatioPromise = Promise[SamplingRatio]()

    private val sampleCounter = new AtomicInteger(0)
    private val allSamples = new ConcurrentLinkedQueue[Key]()
    private val sampleDecisionPromise = Promise[SampleDecision]()

    /**
     * [RPC 메서드] Worker가 자신이 가진 레코드 개수를 Master에게 보고할 때 호출
     * @param request RecordCountReport (Worker ID 및 해당 Worker의 총 레코드 개수 포함)
     * @return SamplingRatio (계산된 샘플링 확률)을 Worker에게 전송
     * 내부적으로 SamplingPolicy.calculateSamplingRatio를 호출
     */
    override def reportRecordCount(request: RecordCountReport): Future[SamplingRatio] = {
        val future = samplingRatioPromise.future

        this.synchronized {
            totalRecordSum.addAndGet(request.totalRecordCount)
            val currentCount = reportCounter.incrementAndGet()

            if (currentCount == numWorkers) {
                val totalRecords = totalRecordSum.get()
                val ratio = calculateSamplingRatio(totalRecords)
                
                val response = SamplingRatio(ratio = ratio)
                
                samplingRatioPromise.success(response)
            }
        }
        
        future
    }

    /**
     * [RPC 메서드] Worker가 로컬에서 샘플링한 Key 리스트를 Master에게 보낼 때 호출
     * * Master는 모든 Worker의 샘플을 통합한 후, PivotSelector의 SortSamples와 selectPivots을 호출하여
     * 파티셔닝 경계 키를 결정
     * @param request SampleKeyList (Worker ID 및 샘플링된 Key 리스트 포함)
     * @return SampleDecision (계산된 최종 Pivot Key 리스트를 Worker에게 전송)
     */
    override def sendSampleKeys(request: SampleKeyList): Future[SampleDecision] = {
        val future = sampleDecisionPromise.future

        this.synchronized {
            val keysFromWorker: Seq[Key] = request.keys.map(_.value.toByteArray)
        
            allSamples.addAll(keysFromWorker.asJava)
            
            val currentCount = sampleCounter.incrementAndGet()
            
            if (currentCount == numWorkers) {
                val aggregatedKeys: Seq[Key] = allSamples.asScala.toSeq
                val sortedKeys = sortSamples(aggregatedKeys)
                val pivotKeys: Vector[Key] = selectPivots(sortedKeys)
                val paddedpivots: Vector[Record] = createPaddedPivots(pivotKeys)
                
                val protoPivots: Seq[KeyMessage] = pivotKeys.map { keyArray =>
                    KeyMessage(value = ByteString.copyFrom(keyArray))
                }
                
                val partitionInfo = PartitionInfo(pivots = protoPivots)
                val response = SampleDecision(
                    proceed = true,
                    content = SampleDecision.Content.PartitionInfo(partitionInfo)
                )
                /*
                    TODO: sample된 개수 맞지 않는 경우 처리 (일반적으로 일어나진 않지만 예외처리)
                */
                
                sampleDecisionPromise.success(response)
            }
        }
        
        future
    }

}