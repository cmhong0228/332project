package distributedsorting.worker

import scopt.OptionParser

object Worker {
    def main(args: Array[String]): Unit = {
        WorkerArgsParser.parser.parse(args, WorkerConfig()) match {
            case Some(config) =>
                val Array(masterIp, masterPortStr) = config.masterAddress.split(":")
                val masterPort = masterPortStr.toInt
                
                println(s"Master IP: $masterIp, Port: $masterPort")
                println(s"Input Directories: ${config.inputDirs.mkString(", ")}")
                println(s"Output Directory: ${config.outputDir}")
                
                workerApplication(masterIp, masterPort, config.inputDirs, config.outputDir)

            case None =>
                println("Argument parsing failed.")
        }
    }

    def workerApplication(masterIp: String, masterPort: Int, inputDirs: Seq[String], outputDir: String): Unit = {

    }
}

case class WorkerConfig(
    masterAddress: String = "",
    inputDirs: Seq[String] = Seq.empty,
    outputDir: String = ""
)

object WorkerArgsParser {
    val parser = new OptionParser[WorkerConfig]("worker") {
    
    head("worker", "Distributed Sort Worker Node")

    // 1. Master IP:Port (위치 인자) 처리
    // 첫 번째 위치 인자를 masterAddress로 설정
    arg[String]("<master IP:port>")
      .required()
      .action { (x, c) => c.copy(masterAddress = x) }
      .text("Master IP and port (e.g., 141.223.91.80:30040)")

    // 2. 입력 디렉토리 옵션 (-I) 처리
    // Repetition: 여러 개의 디렉토리를 목록으로 받음
    opt[Seq[String]]('I', "input-directories")
      .required()
      .valueName("<dir1> <dir2>...")
      .unbounded() // 인자가 무한정 올 수 있음을 명시
      .action { (x, c) => c.copy(inputDirs = x) }
      .text("Input directories containing unsorted input blocks.")

    // 3. 출력 디렉토리 옵션 (-O) 처리
    opt[String]('O', "output-directory")
      .required()
      .valueName("<dir>")
      .action { (x, c) => c.copy(outputDir = x) }
      .text("Output directory to store sorted partition files.")
      
    help("help").text("prints this usage text")
  }
}