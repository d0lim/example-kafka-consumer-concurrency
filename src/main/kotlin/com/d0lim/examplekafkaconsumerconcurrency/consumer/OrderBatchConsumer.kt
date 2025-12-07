package com.d0lim.examplekafkaconsumerconcurrency.consumer

import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.service.FailedRecord
import com.d0lim.examplekafkaconsumerconcurrency.service.OrderProcessingService
import com.d0lim.examplekafkaconsumerconcurrency.service.RetryService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class OrderBatchConsumer(
    private val orderProcessingService: OrderProcessingService,
    private val retryService: RetryService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = [KafkaConfig.MAIN_TOPIC],
        containerFactory = "batchContainerFactory",
        groupId = "order-consumer-group"
    )
    fun consumeBatch(
        messages: List<ConsumerRecord<String, OrderMessage>>,
        acknowledgment: Acknowledgment
    ) {
        if (messages.isEmpty()) {
            acknowledgment.acknowledge()
            return
        }

        logger.info("Received batch of {} messages", messages.size)

        try {
            val result = orderProcessingService.processBatch(messages)

            logger.info(
                "Batch processed: {} successful, {} failed",
                result.successful.size,
                result.failed.size
            )

            handleFailures(result.failed)

            acknowledgment.acknowledge()
        } catch (e: Exception) {
            logger.error("Batch processing failed completely", e)
            messages.forEach { record ->
                retryService.sendToRetry(record, e as Exception)
            }
            acknowledgment.acknowledge()
        }
    }

    private fun handleFailures(failures: List<FailedRecord>) {
        failures.forEach { failedRecord ->
            retryService.sendToRetry(failedRecord.record, failedRecord.exception)
        }
    }
}
