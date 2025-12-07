package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderAction
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MessagePartitionerTest {

    private val partitioner = MessagePartitioner()

    @Test
    fun `should separate unique keys from duplicate keys`() {
        val messages = listOf(
            createRecord("key1", "order1"),
            createRecord("key2", "order2"),
            createRecord("key1", "order3"),
            createRecord("key3", "order4"),
            createRecord("key2", "order5"),
            createRecord("key4", "order6")
        )

        val result = partitioner.partition(messages)

        assertEquals(2, result.uniqueKeyMessages.size)
        assertTrue(result.uniqueKeyMessages.any { it.value().orderId == "order4" })
        assertTrue(result.uniqueKeyMessages.any { it.value().orderId == "order6" })

        assertEquals(2, result.duplicateKeyGroups.size)
        assertEquals(2, result.duplicateKeyGroups["key1"]?.size)
        assertEquals(2, result.duplicateKeyGroups["key2"]?.size)
    }

    @Test
    fun `should return all as unique when no duplicates`() {
        val messages = listOf(
            createRecord("key1", "order1"),
            createRecord("key2", "order2"),
            createRecord("key3", "order3")
        )

        val result = partitioner.partition(messages)

        assertEquals(3, result.uniqueKeyMessages.size)
        assertTrue(result.duplicateKeyGroups.isEmpty())
    }

    @Test
    fun `should return all as duplicates when all have same key`() {
        val messages = listOf(
            createRecord("key1", "order1"),
            createRecord("key1", "order2"),
            createRecord("key1", "order3")
        )

        val result = partitioner.partition(messages)

        assertTrue(result.uniqueKeyMessages.isEmpty())
        assertEquals(1, result.duplicateKeyGroups.size)
        assertEquals(3, result.duplicateKeyGroups["key1"]?.size)
    }

    @Test
    fun `should preserve order within duplicate key groups`() {
        val messages = listOf(
            createRecord("key1", "order1"),
            createRecord("key1", "order2"),
            createRecord("key1", "order3")
        )

        val result = partitioner.partition(messages)

        val key1Messages = result.duplicateKeyGroups["key1"]!!
        assertEquals("order1", key1Messages[0].value().orderId)
        assertEquals("order2", key1Messages[1].value().orderId)
        assertEquals("order3", key1Messages[2].value().orderId)
    }

    @Test
    fun `should handle empty list`() {
        val result = partitioner.partition(emptyList())

        assertTrue(result.uniqueKeyMessages.isEmpty())
        assertTrue(result.duplicateKeyGroups.isEmpty())
    }

    private fun createRecord(key: String, orderId: String): ConsumerRecord<String, OrderMessage> {
        return ConsumerRecord(
            "test-topic",
            0,
            0,
            key,
            OrderMessage(
                orderId = orderId,
                customerId = "customer1",
                action = OrderAction.CREATE,
                amount = 100,
                timestamp = Instant.now()
            )
        )
    }
}
