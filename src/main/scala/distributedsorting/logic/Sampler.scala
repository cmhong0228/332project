package distributedsorting.logic

trait Sampler {
    // key의 길이
    val KEY_SIZE: Int

    /**
     * 주어진 확률로 입력 Iterator에서 Key를 샘플링
     * @param inputIterator 입력 레코드 스트림
     * @param samplingRatio 마스터로부터 받은 샘플링 확률 (k/n)
     * @return 샘플링된 Key 리스트 (Seq[Key])
     */
    def sampleKeys(inputIterator: Iterator[Record], samplingRatio: Double): Seq[Key] = ???
}