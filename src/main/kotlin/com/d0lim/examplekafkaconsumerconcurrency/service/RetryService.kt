package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.time.Instant

@Service
class RetryService(
    private val kafkaTemplate: KafkaTemplate<String, OrderMessage>
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val HEADER_RETRY_COUNT = "retry-count"
        const val HEADER_SCHEDULED_TIME = "scheduled-time"
        const val HEADER_ORIGINAL_TOPIC = "original-topic"
        const val HEADER_EXCEPTION = "exception-message"
        const val MAX_RETRY_COUNT = 2
        const val RETRY_DELAY_MS = 3000L
    }

    fun sendToRetry(record: ConsumerRecord<String, OrderMessage>, exception: Exception) {
        val currentRetryCount = getRetryCount(record)

        if (currentRetryCount >= MAX_RETRY_COUNT) {
            sendToDlt(record, exception)
            return
        }

        val scheduledTime = Instant.now().toEpochMilli() + RETRY_DELAY_MS

        val producerRecord = ProducerRecord(
            KafkaConfig.RETRY_TOPIC,
            null,
            record.key(),
            record.value(),
            listOf(
                RecordHeader(HEADER_RETRY_COUNT, (currentRetryCount + 1).toString().toByteArray(StandardCharsets.UTF_8)),
                RecordHeader(HEADER_SCHEDULED_TIME, scheduledTime.toString().toByteArray(StandardCharsets.UTF_8)),
                RecordHeader(HEADER_ORIGINAL_TOPIC, KafkaConfig.MAIN_TOPIC.toByteArray(StandardCharsets.UTF_8)),
                RecordHeader(HEADER_EXCEPTION, exception.message?.toByteArray(StandardCharsets.UTF_8) ?: byteArrayOf())
            )
        )

        kafkaTemplate.send(producerRecord)
        logger.info("Sent message to retry topic: key={}, retryCount={}", record.key(), currentRetryCount + 1)
    }

    fun sendToDlt(record: ConsumerRecord<String, OrderMessage>, exception: Exception) {
        val producerRecord = ProducerRecord(
            KafkaConfig.DLT_TOPIC,
            null,
            record.key(),
            record.value(),
            listOf(
                RecordHeader(HEADER_RETRY_COUNT, getRetryCount(record).toString().toByteArray(StandardCharsets.UTF_8)),
                RecordHeader(HEADER_ORIGINAL_TOPIC, KafkaConfig.MAIN_TOPIC.toByteArray(StandardCharsets.UTF_8)),
                RecordHeader(HEADER_EXCEPTION, exception.message?.toByteArray(StandardCharsets.UTF_8) ?: byteArrayOf())
            )
        )

        kafkaTemplate.send(producerRecord)
        logger.warn("Sent message to DLT: key={}, exception={}", record.key(), exception.message)
    }

    fun getRetryCount(record: ConsumerRecord<String, OrderMessage>): Int {
        val header = record.headers().lastHeader(HEADER_RETRY_COUNT)
        return header?.value()?.let { String(it, StandardCharsets.UTF_8).toIntOrNull() } ?: 0
    }

    fun getScheduledTime(record: ConsumerRecord<String, OrderMessage>): Long {
        val header = record.headers().lastHeader(HEADER_SCHEDULED_TIME)
        return header?.value()?.let { String(it, StandardCharsets.UTF_8).toLongOrNull() } ?: 0L
    }
}
