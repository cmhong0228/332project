package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.logic._
import java.nio.file.Path
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import distributedsorting.distributedsorting.MasterServiceGrpc.MasterServiceStub
import com.google.protobuf.ByteString
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal
import scala.util.{Try, Success, Failure}

trait MasterClient { self: RecordCountCalculator with RecordExtractor => 
    implicit val ec: ExecutionContext
    // Master와의 통신 스텁 및 Worker ID 정의
    val masterIp: String
    val masterPort: Int
    val workerInfo: WorkerInfo
    private lazy val channel: ManagedChannel = ManagedChannelBuilder.forAddress(masterIp, masterPort)
      .usePlaintext()
      .build()
    private lazy val masterClient: MasterServiceStub = MasterServiceGrpc.stub(channel)

    /** 채널 종료 함수 */
    def cleanup(): Unit = {
        channel.shutdownNow()
    }

    //
    private var _allWorkers: Seq[WorkerInfo] = Seq.empty[WorkerInfo]

    def getAllWorkers: Seq[WorkerInfo] = this.synchronized { _allWorkers }

    // ======================= Registration =======================
    def registerWorker(): Unit = {
        def attemptCall(): Future[RegisterResponse] = {
            try {
                masterClient.registerWorker(workerInfo) 
            } catch {
                case NonFatal(e) => 
                    Future.failed(e)
            }
        }

        var success = false
        val timeOut = Duration.Inf
    
        while (!success) {
            val result = Try(Await.result(attemptCall(), timeOut)) // TODO: 대기시간 설정

            result match {
                case Success(response) =>
                    if (response.success) {
                        this.synchronized {
                            _allWorkers = response.workerList
                        }
                        success = true
                    } else {
                        // TODO: 예외처리
                    }

                case Failure(e) => {
                    // TODO: 예외처리
                }
            }

            if (!success) {
                Thread.sleep(100)
            }
        }
    }
            
    // ======================= Termination =======================
    def ReportCompletion(): Unit = {
        val timeOut = Duration.Inf
        val reportFuture: Future[CompletionResponse] = masterClient.reportCompletion()
    
        val result = Try(Await.result(reportFuture, timeOut)) 

        result match {
            case Success(response) if response.success =>
                ()
                // 성공 시, 필요한 경우 추가

            case _ =>
                () 
                // 실패 시, 예외처리 필요한 경우 추가
        }
        
        cleanup()
    }

    // ======================= Sampling =======================
    /*
     * Sampling(Worker)
     * 역할
     * * master에게 자신이 갖고있는 data 전달 후 sample ratio 받음
     * * master에게 sample data 보낸 후 pivots 받음
     * pivots: Vector[key]
     */


    /**
     * Worker 노드의 샘플링 프로세스를 실행하는 함수
     * Master와의 2단계 통신(보고 -> 비율 획득, 샘플 전송 -> 피벗 획득)을 조정하고, 최종 피벗을 반환
     * @param inputDirs Worker가 처리할 데이터가 위치한 로컬 디스크 경로 목록
     * @return Master로부터 받은, 데이터를 파티셔닝할 최종 경계 Key 벡터 (Pivots)
     */
    def executeSampling(inputDirs: Seq[Path]): Vector[Record] = {
        
        // --- 1단계: 레코드 수 보고 및 샘플링 비율 획득 ---

        // 1. 로컬 레코드 수 계산 (self-type 능력 사용)
        val localRecordCount = self.calculateTotalRecords(inputDirs)

        // 2. gRPC 요청 생성
        val report = RecordCountReport(
            workerId = workerInfo.workerId,
            totalRecordCount = localRecordCount
        )

        // 3. 마스터에게 보고 (gRPC 호출)
        val futureRatio: Future[SamplingRatio] = masterClient.reportRecordCount(report)

        // 4. (블로킹) 마스터가 모든 워커의 보고를 받을 때까지 대기
        // (10분은 매우 넉넉한 타임아웃이며, 실제로는 Config에서 관리해야 함)
        val samplingRatio = Await.result(futureRatio, 10.minutes)
        val ratio = samplingRatio.ratio

        // --- 2단계: 샘플 전송 및 피벗 획득 ---

        // 5. 획득한 비율로 로컬 샘플 추출 (self-type 능력 사용)
        val localSamples: Seq[Key] = self.readAndExtractSamples(inputDirs, ratio)

        // 6. Scala의 Key(Array[Byte])를 Protobuf의 KeyMessage(ByteString)로 변환
        val protoKeys: Seq[KeyMessage] = localSamples.map { keyArray =>
            KeyMessage(value = ByteString.copyFrom(keyArray))
        }

        // 7. gRPC 요청 생성
        val sampleList = SampleKeyList(
            workerId = workerInfo.workerId,
            keys = protoKeys
        )

        // 8. 마스터에게 샘플 전송 (gRPC 호출)
        val futureDecision: Future[SampleDecision] = masterClient.sendSampleKeys(sampleList)

        // 9. (블로킹) 마스터가 모든 샘플을 취합하고 피벗을 계산할 때까지 대기
        val decision = Await.result(futureDecision, 10.minutes)

        // 10. 마스터의 결정 처리 (proceed=true 가정)
        decision.content match {
            case SampleDecision.Content.PartitionInfo(partitionInfo) =>
                // 11. Protobuf의 KeyMessage를 Scala의 Key(Array[Byte])로 변환
                val pivots: Vector[Record] = partitionInfo.pivots.map { keyMsg =>
                    keyMsg.value.toByteArray
                }.toVector
                
                pivots

            case SampleDecision.Content.Control(control) =>
                // 재샘플링 로직 (여기서는 예외 처리)
                throw new RuntimeException(s"Master requested re-sampling with new ratio ${control.newRatio}. This is not implemented.")
            
            case SampleDecision.Content.Empty =>
                throw new RuntimeException("Master returned an empty sample decision.")
        }
    }
}