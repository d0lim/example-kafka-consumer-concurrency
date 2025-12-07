package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderAction
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderEntity
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderStatus
import com.d0lim.examplekafkaconsumerconcurrency.domain.ProcessedAction
import com.d0lim.examplekafkaconsumerconcurrency.repository.OrderRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

data class ProcessingResult(
    val successful: List<ConsumerRecord<String, OrderMessage>>,
    val failed: List<FailedRecord>
)

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

        val uniqueResult = uniqueKeyFuture.join()
        val duplicateResult = duplicateKeyFuture.join()

        return ProcessingResult(
            successful = uniqueResult.successful + duplicateResult.successful,
            failed = uniqueResult.failed + duplicateResult.failed
        )
    }

    private fun processUniqueKeys(messages: List<ConsumerRecord<String, OrderMessage>>): ProcessingResult {
        if (messages.isEmpty()) {
            return ProcessingResult(emptyList(), emptyList())
        }

        val successful = mutableListOf<ConsumerRecord<String, OrderMessage>>()
        val failed = mutableListOf<FailedRecord>()

        val (failingMessages, normalMessages) = messages.partition {
            failureSimulator.shouldFail(it.value().orderId)
        }

        failingMessages.forEach { record ->
            failureSimulator.recordFailure(record.value().orderId)
            failed.add(FailedRecord(record, SimulatedFailureException("Simulated failure for order: ${record.value().orderId}")))
        }

        if (normalMessages.isNotEmpty()) {
            try {
                val entities = normalMessages.map { record ->
                    toNewEntity(record.value())
                }
                orderRepository.saveAll(entities)
                successful.addAll(normalMessages)
                logger.debug("Bulk inserted {} unique key messages", normalMessages.size)
            } catch (e: Exception) {
                logger.warn("Bulk insert failed, falling back to individual processing", e)
                normalMessages.forEach { record ->
                    try {
                        val entity = toNewEntity(record.value())
                        orderRepository.save(entity)
                        successful.add(record)
                    } catch (ex: Exception) {
                        failed.add(FailedRecord(record, ex as Exception))
                    }
                }
            }
        }

        return ProcessingResult(successful, failed)
    }

    private fun processDuplicateKeys(
        groups: Map<String, List<ConsumerRecord<String, OrderMessage>>>
    ): ProcessingResult {
        if (groups.isEmpty()) {
            return ProcessingResult(emptyList(), emptyList())
        }

        val futures = groups.map { (key, records) ->
            CompletableFuture.supplyAsync({
                processKeySequentially(key, records)
            }, virtualThreadExecutor)
        }

        val results = futures.map { it.join() }

        return ProcessingResult(
            successful = results.flatMap { it.successful },
            failed = results.flatMap { it.failed }
        )
    }

    private fun processKeySequentially(
        key: String,
        records: List<ConsumerRecord<String, OrderMessage>>
    ): ProcessingResult {
        val successful = mutableListOf<ConsumerRecord<String, OrderMessage>>()
        val failed = mutableListOf<FailedRecord>()

        val orderId = records.first().value().orderId
        var currentEntity = orderRepository.findById(orderId).orElse(null)

        records.forEachIndexed { index, record ->
            try {
                currentEntity = applyMessage(currentEntity, record.value(), index)
                orderRepository.save(currentEntity!!)
                successful.add(record)
                logger.debug("Processed message {} for key {}", index, key)
            } catch (e: Exception) {
                failed.add(FailedRecord(record, e as Exception))
                logger.error("Failed to process message for key {}: {}", key, e.message)
            }
        }

        return ProcessingResult(successful, failed)
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

    private fun toNewEntity(message: OrderMessage): OrderEntity {
        val status = when (message.action) {
            OrderAction.CREATE -> OrderStatus.CREATED
            OrderAction.UPDATE -> OrderStatus.UPDATED
            OrderAction.CANCEL -> OrderStatus.CANCELLED
        }

        return OrderEntity(
            orderId = message.orderId,
            customerId = message.customerId,
            status = status,
            amount = message.amount,
            processedActions = mutableListOf(
                ProcessedAction(
                    action = message.action,
                    amount = message.amount,
                    processedAt = Instant.now(),
                    sequenceNumber = 0
                )
            )
        )
    }

    private fun applyMessage(
        existingEntity: OrderEntity?,
        message: OrderMessage,
        sequenceNumber: Int
    ): OrderEntity {
        val now = Instant.now()
        val newAction = ProcessedAction(
            action = message.action,
            amount = message.amount,
            processedAt = now,
            sequenceNumber = sequenceNumber
        )

        return if (existingEntity == null) {
            OrderEntity(
                orderId = message.orderId,
                customerId = message.customerId,
                status = toStatus(message.action),
                amount = message.amount,
                processedActions = mutableListOf(newAction),
                createdAt = now,
                updatedAt = now
            )
        } else {
            existingEntity.copy(
                status = toStatus(message.action),
                amount = when (message.action) {
                    OrderAction.UPDATE -> existingEntity.amount + message.amount
                    OrderAction.CANCEL -> 0
                    OrderAction.CREATE -> message.amount
                },
                processedActions = existingEntity.processedActions.apply { add(newAction) },
                updatedAt = now
            )
        }
    }

    private fun toStatus(action: OrderAction): OrderStatus {
        return when (action) {
            OrderAction.CREATE -> OrderStatus.CREATED
            OrderAction.UPDATE -> OrderStatus.UPDATED
            OrderAction.CANCEL -> OrderStatus.CANCELLED
        }
    }
}
