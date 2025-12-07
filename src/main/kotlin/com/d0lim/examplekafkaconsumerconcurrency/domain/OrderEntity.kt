package com.d0lim.examplekafkaconsumerconcurrency.domain

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document(collection = "orders")
data class OrderEntity(
    @Id
    val orderId: String,
    val customerId: String,
    val status: OrderStatus,
    val amount: Long,
    val processedActions: List<ProcessedAction> = emptyList(),
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now(),
    @Version
    val version: Long? = null
) {
    fun applyAction(message: OrderMessage, sequenceNumber: Int): OrderEntity {
        val now = Instant.now()
        val newAction = ProcessedAction(
            action = message.action,
            amount = message.amount,
            processedAt = now,
            sequenceNumber = sequenceNumber
        )

        return copy(
            status = message.action.toStatus(),
            amount = calculateNewAmount(message),
            processedActions = processedActions + newAction,
            updatedAt = now
        )
    }

    private fun calculateNewAmount(message: OrderMessage): Long = when (message.action) {
        OrderAction.UPDATE -> amount + message.amount
        OrderAction.CANCEL -> 0
        OrderAction.CREATE -> message.amount
    }

    companion object {
        fun createFrom(message: OrderMessage): OrderEntity {
            val now = Instant.now()
            val action = ProcessedAction(
                action = message.action,
                amount = message.amount,
                processedAt = now,
                sequenceNumber = 0
            )

            return OrderEntity(
                orderId = message.orderId,
                customerId = message.customerId,
                status = message.action.toStatus(),
                amount = message.amount,
                processedActions = listOf(action),
                createdAt = now,
                updatedAt = now
            )
        }
    }
}

data class ProcessedAction(
    val action: OrderAction,
    val amount: Long,
    val processedAt: Instant,
    val sequenceNumber: Int
)

enum class OrderStatus {
    CREATED,
    UPDATED,
    CANCELLED
}
