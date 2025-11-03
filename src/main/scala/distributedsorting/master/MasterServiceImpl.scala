package distributedsorting.master

import distributedsorting.distributedsorting._
import scala.concurrent.{Future, Promise}

class MasterServiceImpl(numWorkers: Int) extends MasterServiceGrpc.MasterService {

    // ======================= Sampling =======================
    /*
     * Sampling(Master)
     * 역할
     * * worker로 부터 Record 개수를 받아 sampling ratio를 계산 후 전송
     * * worker로 부터 Sampling 된 records를 받아 pivots을 구한 후 전송
     */
     
    private val KEY_LENGTH = ???
    private val RECORD_LENGTH = ???

    override def reportRecordCount(request: RecordCountReport): Future[SamplingRatio] = ???

    override def sendSampleKeys(request: SampleKeyList): Future[SampleDecision] = ???

    private def calculateSamplingRatio(numTotalRecords: Long, numSampleRecords: Long): Double = ???

    private def SortSamples(samples: Seq[Key]): Seq[Key] = ???

    private def selectPivots(sortedSamples: Seq[Key], numWorkers: Int): Vector[Key] = ???
}