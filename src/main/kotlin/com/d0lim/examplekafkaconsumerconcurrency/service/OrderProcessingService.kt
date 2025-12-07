package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderEntity
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.repository.OrderRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

data class ProcessingResult(
    val successful: List<ConsumerRecord<String, OrderMessage>>,
    val failed: List<FailedRecord>
) {
    operator fun plus(other: ProcessingResult): ProcessingResult =
        ProcessingResult(
            successful = successful + other.successful,
            failed = failed + other.failed
        )

    companion object {
        val EMPTY = ProcessingResult(emptyList(), emptyList())
    }
}

data class FailedRecord(
    val record: ConsumerRecord<String, OrderMessage>,
    val exception: Exception
)

class SimulatedFailureException(message: String) : RuntimeException(message)

@Service
class OrderProcessingService(
    private val orderRepository: OrderRepository,
    private val messagePartitioner: MessagePartitioner,
    private val failureSimulator: FailureSimulator
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()

    fun processBatch(messages: List<ConsumerRecord<String, OrderMessage>>): ProcessingResult {
        val partitioned = messagePartitioner.partition(messages)

        val uniqueKeyFuture = CompletableFuture.supplyAsync({
            processUniqueKeys(partitioned.uniqueKeyMessages)
        }, virtualThreadExecutor)

        val duplicateKeyFuture = CompletableFuture.supplyAsync({
            processDuplicateKeys(partitioned.duplicateKeyGroups)
        }, virtualThreadExecutor)

        return uniqueKeyFuture.join() + duplicateKeyFuture.join()
    }

    private fun processUniqueKeys(messages: List<ConsumerRecord<String, OrderMessage>>): ProcessingResult {
        if (messages.isEmpty()) {
            return ProcessingResult.EMPTY
        }

        val (failingMessages, normalMessages) = messages.partition {
            failureSimulator.shouldFail(it.value().orderId)
        }

        val failedResults = failingMessages.map { record ->
            failureSimulator.recordFailure(record.value().orderId)
            FailedRecord(record, SimulatedFailureException("Simulated failure for order: ${record.value().orderId}"))
        }

        val normalResults = processBulkInsert(normalMessages)

        return ProcessingResult(
            successful = normalResults.successful,
            failed = failedResults + normalResults.failed
        )
    }

    private fun processBulkInsert(messages: List<ConsumerRecord<String, OrderMessage>>): ProcessingResult {
        if (messages.isEmpty()) {
            return ProcessingResult.EMPTY
        }

        return try {
            val entities = messages.map { OrderEntity.createFrom(it.value()) }
            orderRepository.saveAll(entities)
            logger.debug("Bulk inserted {} unique key messages", messages.size)
            ProcessingResult(successful = messages, failed = emptyList())
        } catch (e: Exception) {
            logger.warn("Bulk insert failed, falling back to individual processing", e)
            processIndividually(messages)
        }
    }

    private fun processIndividually(messages: List<ConsumerRecord<String, OrderMessage>>): ProcessingResult {
        val results = messages.map { record ->
            runCatching {
                val entity = OrderEntity.createFrom(record.value())
                orderRepository.save(entity)
                record
            }
        }

        return ProcessingResult(
            successful = results.filter { it.isSuccess }.map { it.getOrThrow() },
            failed = results.filter { it.isFailure }.mapIndexed { index, result ->
                FailedRecord(messages[index], result.exceptionOrNull() as Exception)
            }
        )
    }

    private fun processDuplicateKeys(
        groups: Map<String, List<ConsumerRecord<String, OrderMessage>>>
    ): ProcessingResult {
        if (groups.isEmpty()) {
            return ProcessingResult.EMPTY
        }

        val futures = groups.map { (key, records) ->
            CompletableFuture.supplyAsync({
                processKeySequentially(key, records)
            }, virtualThreadExecutor)
        }

        return futures
            .map { it.join() }
            .fold(ProcessingResult.EMPTY) { acc, result -> acc + result }
    }

    private fun processKeySequentially(
        key: String,
        records: List<ConsumerRecord<String, OrderMessage>>
    ): ProcessingResult {
        val orderId = records.first().value().orderId
        val initialEntity = orderRepository.findById(orderId).orElse(null)

        return records.foldIndexed(
            SequentialProcessingState(entity = initialEntity)
        ) { index, state, record ->
            processRecord(state, record, index, key)
        }.toResult()
    }

    private fun processRecord(
        state: SequentialProcessingState,
        record: ConsumerRecord<String, OrderMessage>,
        index: Int,
        key: String
    ): SequentialProcessingState {
        return try {
            val updatedEntity = applyMessage(state.entity, record.value(), index)
            val savedEntity = orderRepository.save(updatedEntity)
            logger.debug("Processed message {} for key {}", index, key)
            state.withSuccess(record, savedEntity)
        } catch (e: Exception) {
            logger.error("Failed to process message for key {}: {}", key, e.message)
            state.withFailure(record, e)
        }
    }

    fun processSingleMessage(message: OrderMessage): OrderEntity {
        checkForSimulatedFailure(message.orderId)
        val existingEntity = orderRepository.findById(message.orderId).orElse(null)
        val updatedEntity = applyMessage(existingEntity, message, 0)
        return orderRepository.save(updatedEntity)
    }

    private fun checkForSimulatedFailure(orderId: String) {
        if (failureSimulator.shouldFail(orderId)) {
            failureSimulator.recordFailure(orderId)
            throw SimulatedFailureException("Simulated failure for order: $orderId")
        }
    }

    private fun applyMessage(
        existingEntity: OrderEntity?,
        message: OrderMessage,
        sequenceNumber: Int
    ): OrderEntity {
        return existingEntity?.applyAction(message, sequenceNumber)
            ?: OrderEntity.createFrom(message)
    }
}

private data class SequentialProcessingState(
    val entity: OrderEntity? = null,
    val successful: List<ConsumerRecord<String, OrderMessage>> = emptyList(),
    val failed: List<FailedRecord> = emptyList()
) {
    fun withSuccess(record: ConsumerRecord<String, OrderMessage>, updatedEntity: OrderEntity): SequentialProcessingState =
        copy(
            entity = updatedEntity,
            successful = successful + record
        )

    fun withFailure(record: ConsumerRecord<String, OrderMessage>, exception: Exception): SequentialProcessingState =
        copy(failed = failed + FailedRecord(record, exception))

    fun toResult(): ProcessingResult = ProcessingResult(successful, failed)
}
