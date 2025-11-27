package distributedsorting.master

import scopt.OptionParser
import java.net.InetAddress
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import distributedsorting.distributedsorting._

object Master {
    def main(args: Array[String]): Unit = {        
        MasterArgsParser.parser.parse(args, MasterConfig()) match {
            case Some(config) if config.numWorkers > 0 =>
                val masterApp = new MasterApp(config.numWorkers)
                masterApp.run()
                masterApp.blockUntilShutdown()
            case _ => ()
        }
    }
}

class MasterApp (numWorkers: Int) extends ShutdownController { 
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
        
        println(s"$masterIp:$masterPort") 
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
            server.shutdownNow()
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