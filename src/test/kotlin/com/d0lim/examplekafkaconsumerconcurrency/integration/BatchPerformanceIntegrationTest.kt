package com.d0lim.examplekafkaconsumerconcurrency.integration

import com.d0lim.examplekafkaconsumerconcurrency.TestcontainersConfiguration
import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderAction
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.repository.OrderRepository
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class BatchPerformanceIntegrationTest {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, OrderMessage>

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @BeforeEach
    fun setUp() {
        orderRepository.deleteAll()
    }

    @Test
    fun `should process large batch of unique keys efficiently with bulk insert`() {
        val messageCount = 100
        val baseTime = System.currentTimeMillis()

        val messages = (1..messageCount).map { i ->
            val orderId = "unique-order-$baseTime-$i"
            OrderMessage(
                orderId = orderId,
                customerId = "customer-$i",
                action = OrderAction.CREATE,
                amount = 100L + i,
                timestamp = Instant.now()
            )
        }

        val startTime = System.currentTimeMillis()

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, message.orderId, message))
        }

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val count = orderRepository.count()
                assertEquals(messageCount.toLong(), count, "Should have processed all $messageCount messages")
            }

        val processingTime = System.currentTimeMillis() - startTime
        logger.info("Processed {} unique key messages in {}ms", messageCount, processingTime)

        val orders = orderRepository.findAll()
        assertEquals(messageCount, orders.size)

        orders.forEach { order ->
            assertEquals(1, order.processedActions.size)
            assertEquals(OrderAction.CREATE, order.processedActions[0].action)
        }
    }

    @Test
    fun `should process mixed batch with unique and duplicate keys`() {
        val baseTime = System.currentTimeMillis()

        val uniqueMessages = (1..50).map { i ->
            val orderId = "unique-$baseTime-$i"
            OrderMessage(orderId, "customer-$i", OrderAction.CREATE, 100L, Instant.now())
        }

        val duplicateMessages = (1..10).flatMap { i ->
            val orderId = "duplicate-$baseTime-$i"
            listOf(
                OrderMessage(orderId, "customer-$i", OrderAction.CREATE, 100L, Instant.now()),
                OrderMessage(orderId, "customer-$i", OrderAction.UPDATE, 50L, Instant.now().plusMillis(1)),
                OrderMessage(orderId, "customer-$i", OrderAction.UPDATE, 25L, Instant.now().plusMillis(2))
            )
        }

        val allMessages = uniqueMessages + duplicateMessages

        allMessages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, message.orderId, message))
        }

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val count = orderRepository.count()
                assertEquals(60L, count, "Should have 50 unique + 10 duplicate key orders = 60 total")
            }

        val orders = orderRepository.findAll()
        val uniqueOrders = orders.filter { it.orderId.startsWith("unique-") }
        val duplicateOrders = orders.filter { it.orderId.startsWith("duplicate-") }

        assertEquals(50, uniqueOrders.size)
        assertEquals(10, duplicateOrders.size)

        uniqueOrders.forEach { order ->
            assertEquals(1, order.processedActions.size)
            assertEquals(100L, order.amount)
        }

        duplicateOrders.forEach { order ->
            assertEquals(3, order.processedActions.size, "Order ${order.orderId} should have 3 actions")
            assertEquals(175L, order.amount, "Order ${order.orderId} should have correct amount")
        }
    }

    @Test
    fun `should handle high volume with many duplicate keys`() {
        val baseTime = System.currentTimeMillis()
        val keyCount = 20
        val messagesPerKey = 5

        val messages = (1..keyCount).flatMap { keyIndex ->
            val orderId = "volume-$baseTime-$keyIndex"
            (1..messagesPerKey).map { msgIndex ->
                val action = if (msgIndex == 1) OrderAction.CREATE else OrderAction.UPDATE
                OrderMessage(
                    orderId = orderId,
                    customerId = "customer-$keyIndex",
                    action = action,
                    amount = 10L * msgIndex,
                    timestamp = Instant.now().plusMillis(msgIndex.toLong())
                )
            }
        }

        val startTime = System.currentTimeMillis()

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, message.orderId, message))
        }

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val orders = orderRepository.findAll()
                val fullyProcessed = orders.filter { it.processedActions.size == messagesPerKey }
                assertEquals(keyCount, fullyProcessed.size, "All orders should have $messagesPerKey actions")
            }

        val processingTime = System.currentTimeMillis() - startTime
        logger.info(
            "Processed {} messages ({} keys x {} messages/key) in {}ms",
            keyCount * messagesPerKey, keyCount, messagesPerKey, processingTime
        )

        orderRepository.findAll().forEach { order ->
            assertEquals(messagesPerKey, order.processedActions.size)

            val sortedActions = order.processedActions.sortedBy { it.sequenceNumber }
            assertEquals(OrderAction.CREATE, sortedActions[0].action)
            sortedActions.drop(1).forEach { action ->
                assertEquals(OrderAction.UPDATE, action.action)
            }
        }
    }

    @Test
    fun `should process concurrent batches from different partitions`() {
        val baseTime = System.currentTimeMillis()
        val totalOrders = 30

        val messages = (1..totalOrders).map { i ->
            val orderId = "partition-test-$baseTime-$i"
            OrderMessage(orderId, "customer-$i", OrderAction.CREATE, 100L, Instant.now())
        }

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, message.orderId, message))
        }

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val count = orderRepository.count()
                assertTrue(count >= totalOrders.toLong(), "Should have processed at least $totalOrders orders")
            }

        val orders = orderRepository.findAll()
            .filter { it.orderId.startsWith("partition-test-$baseTime") }

        assertEquals(totalOrders, orders.size)
    }
}
