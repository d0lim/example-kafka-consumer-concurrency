package com.d0lim.examplekafkaconsumerconcurrency.service

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class FailureSimulator {
    private val failingOrderIds = ConcurrentHashMap.newKeySet<String>()
    private val failureCountMap = ConcurrentHashMap<String, Int>()

    fun addFailingOrderId(orderId: String) {
        failingOrderIds.add(orderId)
        failureCountMap[orderId] = 0
    }

    fun removeFailingOrderId(orderId: String) {
        failingOrderIds.remove(orderId)
        failureCountMap.remove(orderId)
    }

    fun shouldFail(orderId: String): Boolean {
        return failingOrderIds.contains(orderId)
    }

    fun recordFailure(orderId: String): Int {
        return failureCountMap.compute(orderId) { _, count -> (count ?: 0) + 1 } ?: 1
    }

    fun getFailureCount(orderId: String): Int {
        return failureCountMap[orderId] ?: 0
    }

    fun clear() {
        failingOrderIds.clear()
        failureCountMap.clear()
    }
}
