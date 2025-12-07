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
    val processedActions: MutableList<ProcessedAction> = mutableListOf(),
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now(),
    @Version
    val version: Long? = null
)

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
