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
        
        //server.awaitTermination()
        blockUntilShutdown()
    }

    def initiateShutdown(): Unit = {
        if (server != null) {
            println("Shutting down the server...")
            server.shutdown()
        }
    }

    def blockUntilShutdown(): Unit = {
        // gRPC 자체 대기(awaitTermination)가 아니라, 
        // 우리가 만든 'shutdownSignal'이 켜질 때까지 무한 대기합니다.
        println("Master is running... Waiting for completion signal.")
        Await.result(shutdownSignal.future, Duration.Inf)
        
        // 3. 대기가 풀리면(신호가 오면) 여기서부터 종료 절차 시작
        stop()
    }

    // 4. 실제 종료 로직 (메인 스레드에서 실행됨)
    private def stop(): Unit = {
        if (server != null) {
            println("Signal received. shutting down gRPC server...")
            server.shutdown() // 우아한 종료 요청
            
            // 기존 작업(응답 전송)이 다 끝날 때까지 최대 10초 기다려줌
            // 응답이 전송되면 10초 안 채우고 즉시 리턴됨
            if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                server.shutdownNow()
            }
            println("Master Stopped.")
        }
        // 프로세스 종료
        System.exit(0)
    }

    // 5. ShutdownController 구현 (ServiceImpl에서 호출함)
    override def signalShutdown(): Unit = {
        println("Completion signal received from Worker.")
        // Promise를 성공시켜서 blockUntilShutdown()의 대기를 풉니다.
        // trySuccess를 써서 여러 번 호출되어도 에러 안 나게 함
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