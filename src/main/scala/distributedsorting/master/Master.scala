package distributedsorting.master

import scopt.OptionParser
import java.net.InetAddress
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import distributedsorting.distributedsorting._
import com.typesafe.scalalogging.LazyLogging
import distributedsorting.util.LoggingConfig

object Master extends LazyLogging {
    def main(args: Array[String]): Unit = {
        // 로깅 설정 적용 (로거 초기화 전에 호출 필수!)
        LoggingConfig.configure()

        MasterArgsParser.parser.parse(args, MasterConfig()) match {
            case Some(config) if config.numWorkers > 0 =>
                val masterApp = new MasterApp(config.numWorkers)
                masterApp.run()
                masterApp.blockUntilShutdown()
            case _ => ()
        }
    }
}

class MasterApp (numWorkers: Int) extends ShutdownController with LazyLogging {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val masterIp: String = InetAddress.getLocalHost.getHostAddress
    val masterService = new MasterServiceImpl(numWorkers, this)

    val server = ServerBuilder.forPort(0)
        .addService(MasterServiceGrpc.bindService(masterService, ec))
        .build()

    val shutdownSignal = Promise[Unit]()

    def run(): Unit = {
        server.start()
        val masterPort = server.getPort

        println(s"$masterIp:$masterPort")  // 이건 스크립트가 파싱하므로 println 유지
        logger.info(s"Master server started at $masterIp:$masterPort")
        logger.info(s"Waiting for $numWorkers workers to connect...")
    }

    def initiateShutdown(): Unit = {
        if (server != null) {
            server.shutdown()
        }
    }

    def blockUntilShutdown(): Unit = {
        Await.result(shutdownSignal.future, Duration.Inf)
        
        stop()
    }

    private def stop(): Unit = {
        if (server != null) {
            server.shutdown()
        }
        System.exit(0)
    }

    override def signalShutdown(): Unit = {
        shutdownSignal.trySuccess(())
    }
}

case class MasterConfig(numWorkers: Int = 0)

object MasterArgsParser {
    val parser = new OptionParser[MasterConfig]("master") {
        
        head("master", "Distributed Sort Master Node")

        arg[Int]("<# of workers>")
          .required()
          .action { (x, c) => c.copy(numWorkers = x) }
          .text("The fixed number of worker nodes expected to connect.")
          
        help("help").text("prints this usage text")
    }
}