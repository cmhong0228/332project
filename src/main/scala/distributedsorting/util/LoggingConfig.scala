package distributedsorting.util

import com.typesafe.config.ConfigFactory
import ch.qos.logback.classic.{Level, Logger, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.{ConsoleAppender, FileAppender}
import org.slf4j.LoggerFactory

/**
 * application.conf의 로깅 설정을 읽어서 Logback을 프로그래매틱하게 설정
 */
object LoggingConfig {

    /**
     * application.conf에서 로깅 설정을 읽어서 Logback을 구성
     *
     * 반드시 로거가 사용되기 전에 호출해야 함!
     * (즉, main 함수의 제일 처음에 호출)
     */
    def configure(): Unit = {
        try {
            val config = ConfigFactory.load().getConfig("distributedsorting.logging")

            // 설정 읽기
            val enableConsole = config.getBoolean("enable-console")
            val enableFile = config.getBoolean("enable-file")
            val filePath = config.getString("file-path")
            val levelStr = config.getString("level")

            // LoggerContext 가져오기
            val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

            // 기존 설정 초기화
            loggerContext.reset()

            // Root Logger 가져오기
            val rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
            val level = Level.toLevel(levelStr, Level.INFO)
            rootLogger.setLevel(level)

            // 1. Console Appender 설정
            if (enableConsole) {
                val consoleAppender = new ConsoleAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
                consoleAppender.setContext(loggerContext)
                consoleAppender.setName("STDOUT")

                val consoleEncoder = new PatternLayoutEncoder()
                consoleEncoder.setContext(loggerContext)
                consoleEncoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n")
                consoleEncoder.start()

                consoleAppender.setEncoder(consoleEncoder)
                consoleAppender.start()

                rootLogger.addAppender(consoleAppender)
            }

            // 2. File Appender 설정
            if (enableFile) {
                // 로그 디렉토리 생성
                val logFile = new java.io.File(filePath)
                val logDir = logFile.getParentFile
                if (logDir != null && !logDir.exists()) {
                    logDir.mkdirs()
                }

                val fileAppender = new FileAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
                fileAppender.setContext(loggerContext)
                fileAppender.setName("FILE")
                fileAppender.setFile(filePath)

                val fileEncoder = new PatternLayoutEncoder()
                fileEncoder.setContext(loggerContext)
                fileEncoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n")
                fileEncoder.start()

                fileAppender.setEncoder(fileEncoder)
                fileAppender.start()

                rootLogger.addAppender(fileAppender)
            }

            // 3. 특정 패키지 로그 레벨 설정
            loggerContext.getLogger("io.grpc").setLevel(Level.WARN)
            loggerContext.getLogger("io.netty").setLevel(Level.WARN)

        } catch {
            case e: Exception =>
                // 설정 로드 실패 시 기본 logback.xml 사용
                System.err.println(s"Warning: Failed to configure logging: ${e.getMessage}")
                System.err.println("Using default logback.xml configuration")
        }
    }
}
