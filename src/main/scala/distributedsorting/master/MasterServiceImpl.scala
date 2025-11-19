package distributedsorting.master

import distributedsorting.distributedsorting._
import distributedsorting.logic._
import scala.concurrent.{Future, Promise}

class MasterServiceImpl(val numWorkers: Int) extends MasterServiceGrpc.MasterService with SamplingPolicy with PivotSelector{

    // ======================= Sampling =======================
    /*
     * Sampling(Master)
     * 역할
     * * worker로 부터 Record 개수를 받아 sampling ratio를 계산 후 전송
     * * worker로 부터 Sampling 된 records를 받아 pivots을 구한 후 전송
     */
     
    // key의 길이
    val KEY_SIZE: Int = ???
    // memory의 크기
    val MEMORY_SIZE: Long = ???
    // 최대 사용할 메모리의 비율
    val MAX_MEMORY_USAGE_RATIO: Double = ???
    // 머신 당 받고자 하는 평균적인 바이트 수
    val BYTES_PER_MACHINE: Long = ???
    // 머신 마다 넘지 않도록 설계한 바이트 수
    val MAX_BYTES_PER_MACHINE_DESIGNED: Long = ???
    // 정렬 시 사용할 ordering
    val ordering: Ordering[Key] = ???

    /**
     * [RPC 메서드] Worker가 자신이 가진 레코드 개수를 Master에게 보고할 때 호출
     * @param request RecordCountReport (Worker ID 및 해당 Worker의 총 레코드 개수 포함)
     * @return SamplingRatio (계산된 샘플링 확률)을 Worker에게 전송
     * 내부적으로 SamplingPolicy.calculateSamplingRatio를 호출
     */
    override def reportRecordCount(request: RecordCountReport): Future[SamplingRatio] = ???

    /**
     * [RPC 메서드] Worker가 로컬에서 샘플링한 Key 리스트를 Master에게 보낼 때 호출
     * * Master는 모든 Worker의 샘플을 통합한 후, PivotSelector의 SortSamples와 selectPivots을 호출하여
     * 파티셔닝 경계 키를 결정
     * @param request SampleKeyList (Worker ID 및 샘플링된 Key 리스트 포함)
     * @return SampleDecision (계산된 최종 Pivot Key 리스트를 Worker에게 전송)
     */
    override def sendSampleKeys(request: SampleKeyList): Future[SampleDecision] = ???

}