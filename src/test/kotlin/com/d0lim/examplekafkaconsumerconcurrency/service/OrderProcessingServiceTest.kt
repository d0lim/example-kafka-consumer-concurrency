package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderAction
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderEntity
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderStatus
import com.d0lim.examplekafkaconsumerconcurrency.repository.OrderRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import java.time.Instant
import java.util.Optional
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OrderProcessingServiceTest {

    @Mock
    private lateinit var orderRepository: OrderRepository

    private lateinit var messagePartitioner: MessagePartitioner
    private lateinit var failureSimulator: FailureSimulator
    private lateinit var orderProcessingService: OrderProcessingService

    @BeforeEach
    fun setUp() {
        messagePartitioner = MessagePartitioner()
        failureSimulator = FailureSimulator()
        orderProcessingService = OrderProcessingService(
            orderRepository,
            messagePartitioner,
            failureSimulator
        )
    }

    @Test
    fun `should process batch with unique keys using bulk insert`() {
        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key2", "order2", OrderAction.CREATE, 200),
            createRecord("key3", "order3", OrderAction.CREATE, 300)
        )

        whenever(orderRepository.saveAll(any<List<OrderEntity>>())).thenAnswer { it.arguments[0] }

        val result = orderProcessingService.processBatch(messages)

        assertEquals(3, result.successful.size)
        assertTrue(result.failed.isEmpty())
        verify(orderRepository, times(1)).saveAll(any<List<OrderEntity>>())
    }

    @Test
    fun `should process batch with duplicate keys sequentially`() {
        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key1", "order1", OrderAction.UPDATE, 50),
            createRecord("key1", "order1", OrderAction.UPDATE, 25)
        )

        whenever(orderRepository.findById("order1")).thenReturn(Optional.empty())
        whenever(orderRepository.save(any<OrderEntity>())).thenAnswer { it.arguments[0] }

        val result = orderProcessingService.processBatch(messages)

        assertEquals(3, result.successful.size)
        assertTrue(result.failed.isEmpty())
        verify(orderRepository, times(3)).save(any<OrderEntity>())
    }

    @Test
    fun `should handle mixed batch with unique and duplicate keys`() {
        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key2", "order2", OrderAction.CREATE, 200),
            createRecord("key1", "order1", OrderAction.UPDATE, 50)
        )

        whenever(orderRepository.saveAll(any<List<OrderEntity>>())).thenAnswer { it.arguments[0] }
        whenever(orderRepository.findById("order1")).thenReturn(Optional.empty())
        whenever(orderRepository.save(any<OrderEntity>())).thenAnswer { it.arguments[0] }

        val result = orderProcessingService.processBatch(messages)

        assertEquals(3, result.successful.size)
        assertTrue(result.failed.isEmpty())

        verify(orderRepository, times(1)).saveAll(any<List<OrderEntity>>())
        verify(orderRepository, times(2)).save(any<OrderEntity>())
    }

    @Test
    fun `should apply actions in correct order for duplicate keys`() {
        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key1", "order1", OrderAction.UPDATE, 50),
            createRecord("key1", "order1", OrderAction.UPDATE, 25)
        )

        whenever(orderRepository.findById("order1")).thenReturn(Optional.empty())
        val savedEntities = mutableListOf<OrderEntity>()
        whenever(orderRepository.save(any<OrderEntity>())).thenAnswer {
            val entity = it.arguments[0] as OrderEntity
            savedEntities.add(entity)
            entity
        }

        orderProcessingService.processBatch(messages)

        assertEquals(3, savedEntities.size)

        assertEquals(100, savedEntities[0].amount)
        assertEquals(OrderStatus.CREATED, savedEntities[0].status)

        assertEquals(150, savedEntities[1].amount)
        assertEquals(OrderStatus.UPDATED, savedEntities[1].status)

        assertEquals(175, savedEntities[2].amount)
        assertEquals(OrderStatus.UPDATED, savedEntities[2].status)
    }

    @Test
    fun `should handle CANCEL action correctly`() {
        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key1", "order1", OrderAction.UPDATE, 50),
            createRecord("key1", "order1", OrderAction.CANCEL, 0)
        )

        whenever(orderRepository.findById("order1")).thenReturn(Optional.empty())
        val savedEntities = mutableListOf<OrderEntity>()
        whenever(orderRepository.save(any<OrderEntity>())).thenAnswer {
            val entity = it.arguments[0] as OrderEntity
            savedEntities.add(entity)
            entity
        }

        orderProcessingService.processBatch(messages)

        val lastEntity = savedEntities.last()
        assertEquals(0, lastEntity.amount)
        assertEquals(OrderStatus.CANCELLED, lastEntity.status)
    }

    @Test
    fun `should mark simulated failures as failed`() {
        val failingOrderId = "failing-order"
        failureSimulator.addFailingOrderId(failingOrderId)

        val messages = listOf(
            createRecord("key1", "order1", OrderAction.CREATE, 100),
            createRecord("key2", failingOrderId, OrderAction.CREATE, 200)
        )

        whenever(orderRepository.saveAll(any<List<OrderEntity>>())).thenAnswer { it.arguments[0] }

        val result = orderProcessingService.processBatch(messages)

        assertEquals(1, result.successful.size)
        assertEquals(1, result.failed.size)
        assertEquals(failingOrderId, result.failed[0].record.value().orderId)
    }

    @Test
    fun `should process single message successfully`() {
        val message = OrderMessage(
            orderId = "order1",
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        whenever(orderRepository.findById("order1")).thenReturn(Optional.empty())
        whenever(orderRepository.save(any<OrderEntity>())).thenAnswer { it.arguments[0] }

        val result = orderProcessingService.processSingleMessage(message)

        assertEquals("order1", result.orderId)
        assertEquals(100, result.amount)
        assertEquals(OrderStatus.CREATED, result.status)
    }

    @Test
    fun `should throw exception for simulated single message failure`() {
        val failingOrderId = "failing-order"
        failureSimulator.addFailingOrderId(failingOrderId)

        val message = OrderMessage(
            orderId = failingOrderId,
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        try {
            orderProcessingService.processSingleMessage(message)
            assertTrue(false, "Should have thrown exception")
        } catch (e: SimulatedFailureException) {
            assertTrue(e.message?.contains(failingOrderId) == true)
        }

        verify(orderRepository, never()).save(any())
    }

    @Test
    fun `should handle empty batch`() {
        val result = orderProcessingService.processBatch(emptyList())

        assertTrue(result.successful.isEmpty())
        assertTrue(result.failed.isEmpty())
    }

    private fun createRecord(
        key: String,
        orderId: String,
        action: OrderAction,
        amount: Long
    ): ConsumerRecord<String, OrderMessage> {
        return ConsumerRecord(
            "test-topic",
            0,
            0,
            key,
            OrderMessage(
                orderId = orderId,
                customerId = "customer1",
                action = action,
                amount = amount,
                timestamp = Instant.now()
            )
        )
    }
}
