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
class KeyOrderingIntegrationTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, OrderMessage>

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @BeforeEach
    fun setUp() {
        orderRepository.deleteAll()
    }

    @Test
    fun `should process messages with same key in order`() {
        val orderId = "order-${System.currentTimeMillis()}"
        val key = orderId

        val messages = listOf(
            OrderMessage(orderId, "customer1", OrderAction.CREATE, 100, Instant.now()),
            OrderMessage(orderId, "customer1", OrderAction.UPDATE, 50, Instant.now().plusMillis(1)),
            OrderMessage(orderId, "customer1", OrderAction.UPDATE, 30, Instant.now().plusMillis(2))
        )

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, key, message)).get()
        }

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val order = orderRepository.findById(orderId).orElse(null)
                assertTrue(order != null, "Order should exist")
                assertEquals(3, order.processedActions.size, "Should have processed 3 actions")
            }

        val order = orderRepository.findById(orderId).get()

        assertEquals(OrderAction.CREATE, order.processedActions[0].action)
        assertEquals(100, order.processedActions[0].amount)

        assertEquals(OrderAction.UPDATE, order.processedActions[1].action)
        assertEquals(50, order.processedActions[1].amount)

        assertEquals(OrderAction.UPDATE, order.processedActions[2].action)
        assertEquals(30, order.processedActions[2].amount)

        assertEquals(180, order.amount)
    }

    @Test
    fun `should process multiple keys in parallel while maintaining order within each key`() {
        val baseTime = System.currentTimeMillis()
        val orderIds = (1..5).map { "order-$baseTime-$it" }

        val messages = orderIds.flatMap { orderId ->
            listOf(
                OrderMessage(orderId, "customer1", OrderAction.CREATE, 100, Instant.now()),
                OrderMessage(orderId, "customer1", OrderAction.UPDATE, 50, Instant.now().plusMillis(1)),
                OrderMessage(orderId, "customer1", OrderAction.UPDATE, 25, Instant.now().plusMillis(2))
            )
        }

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, message.orderId, message)).get()
        }

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                orderIds.forEach { orderId ->
                    val order = orderRepository.findById(orderId).orElse(null)
                    assertTrue(order != null, "Order $orderId should exist")
                    assertEquals(3, order.processedActions.size, "Order $orderId should have 3 actions")
                }
            }

        orderIds.forEach { orderId ->
            val order = orderRepository.findById(orderId).get()

            assertEquals(OrderAction.CREATE, order.processedActions[0].action)
            assertEquals(OrderAction.UPDATE, order.processedActions[1].action)
            assertEquals(OrderAction.UPDATE, order.processedActions[2].action)

            assertEquals(175, order.amount)
        }
    }

    @Test
    fun `should correctly handle CANCEL action after CREATE and UPDATE`() {
        val orderId = "order-cancel-${System.currentTimeMillis()}"
        val key = orderId

        val messages = listOf(
            OrderMessage(orderId, "customer1", OrderAction.CREATE, 100, Instant.now()),
            OrderMessage(orderId, "customer1", OrderAction.UPDATE, 50, Instant.now().plusMillis(1)),
            OrderMessage(orderId, "customer1", OrderAction.CANCEL, 0, Instant.now().plusMillis(2))
        )

        messages.forEach { message ->
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, key, message)).get()
        }

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val order = orderRepository.findById(orderId).orElse(null)
                assertTrue(order != null, "Order should exist")
                assertEquals(3, order.processedActions.size, "Should have processed 3 actions")
            }

        val order = orderRepository.findById(orderId).get()

        assertEquals(0, order.amount)
        assertEquals(OrderAction.CANCEL, order.processedActions.last().action)
    }
}
