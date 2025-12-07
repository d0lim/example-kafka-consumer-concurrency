package com.d0lim.examplekafkaconsumerconcurrency.consumer

import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.service.RetryService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList

@Component
class DltConsumer {
    private val logger = LoggerFactory.getLogger(javaClass)

    val dltMessages: MutableList<ConsumerRecord<String, OrderMessage>> = CopyOnWriteArrayList()

    @KafkaListener(
        topics = [KafkaConfig.DLT_TOPIC],
        containerFactory = "retryContainerFactory",
        groupId = "order-dlt-group"
    )
    fun consumeDlt(
        record: ConsumerRecord<String, OrderMessage>,
        acknowledgment: Acknowledgment
    ) {
        val exceptionMessage = record.headers()
            .lastHeader(RetryService.HEADER_EXCEPTION)
            ?.value()
            ?.let { String(it, StandardCharsets.UTF_8) }
            ?: "Unknown"

        val retryCount = record.headers()
            .lastHeader(RetryService.HEADER_RETRY_COUNT)
            ?.value()
            ?.let { String(it, StandardCharsets.UTF_8) }
            ?: "0"

        logger.error(
            "DLT received: key={}, orderId={}, retryCount={}, exception={}",
            record.key(),
            record.value().orderId,
            retryCount,
            exceptionMessage
        )

        dltMessages.add(record)

        acknowledgment.acknowledge()
    }

    fun clearDltMessages() {
        dltMessages.clear()
    }
}
