package com.d0lim.examplekafkaconsumerconcurrency.consumer

import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.service.OrderProcessingService
import com.d0lim.examplekafkaconsumerconcurrency.service.RetryService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class RetryConsumer(
    private val orderProcessingService: OrderProcessingService,
    private val retryService: RetryService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = [KafkaConfig.RETRY_TOPIC],
        containerFactory = "retryContainerFactory",
        groupId = "order-retry-group"
    )
    fun consumeRetry(
        record: ConsumerRecord<String, OrderMessage>,
        acknowledgment: Acknowledgment
    ) {
        val scheduledTime = retryService.getScheduledTime(record)
        val currentTime = System.currentTimeMillis()

        if (scheduledTime > currentTime) {
            val waitTime = scheduledTime - currentTime
            logger.debug("Waiting {}ms before processing retry message: key={}", waitTime, record.key())
            Thread.sleep(waitTime)
        }

        val retryCount = retryService.getRetryCount(record)
        logger.info("Processing retry message: key={}, retryCount={}", record.key(), retryCount)

        try {
            orderProcessingService.processSingleMessage(record.value())
            logger.info("Retry successful: key={}", record.key())
            acknowledgment.acknowledge()
        } catch (e: Exception) {
            logger.error("Retry failed: key={}, error={}", record.key(), e.message)
            retryService.sendToRetry(record, e)
            acknowledgment.acknowledge()
        }
    }
}
