package com.d0lim.examplekafkaconsumerconcurrency.domain

import java.time.Instant

data class OrderMessage(
    val orderId: String,
    val customerId: String,
    val action: OrderAction,
    val amount: Long,
    val timestamp: Instant = Instant.now()
)

enum class OrderAction {
    CREATE,
    UPDATE,
    CANCEL
}
