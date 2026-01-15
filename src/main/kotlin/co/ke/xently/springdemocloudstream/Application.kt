@file:OptIn(ExperimentalUuidApi::class)

package co.ke.xently.springdemocloudstream

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.function.StreamBridge
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.util.function.Consumer
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

data class BusinessCreated(val id: Uuid, val name: String)

@Configuration
class ApplicationEventListener {
    companion object {
        private val logger = LoggerFactory.getLogger(ApplicationEventListener::class.java)
    }

    @Bean
    fun businessCreatedConsumer(): Consumer<BusinessCreated> {
        return Consumer {
            logger.info("Received notification of created business: {}", it)
        }
    }
}

@Component
class AnalyticsRepository {
    companion object {
        private val logger = LoggerFactory.getLogger(AnalyticsRepository::class.java)
    }

    fun record(event: BusinessCreated) {
        logger.info("Saving business to analytics repository: {}", event)
    }
}

@Component
class BusinessCreatedListener(private val analyticsRepository: AnalyticsRepository) : Consumer<BusinessCreated> {
    companion object {
        private val logger = LoggerFactory.getLogger(BusinessCreatedListener::class.java)
    }

    override fun accept(event: BusinessCreated) {
        logger.info("Received notification of created business: {}", event)
        analyticsRepository.record(event)
    }
}

@Service
class BusinessService(private val streamBridge: StreamBridge) {
    companion object {
        private val logger = LoggerFactory.getLogger(BusinessService::class.java)
    }

    @Scheduled(cron = "0/10 * * * * *")
    fun createBusiness() {
        logger.info("Creating business...")
        val data = BusinessCreated(Uuid.generateV7(), "Xently")
        streamBridge.send("business-created", data)
        logger.info("Business created")
    }
}

@EnableScheduling
@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
