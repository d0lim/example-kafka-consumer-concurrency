package com.d0lim.examplekafkaconsumerconcurrency.integration

import com.d0lim.examplekafkaconsumerconcurrency.TestcontainersConfiguration
import com.d0lim.examplekafkaconsumerconcurrency.config.KafkaConfig
import com.d0lim.examplekafkaconsumerconcurrency.consumer.DltConsumer
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderAction
import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import com.d0lim.examplekafkaconsumerconcurrency.repository.OrderRepository
import com.d0lim.examplekafkaconsumerconcurrency.service.FailureSimulator
import com.d0lim.examplekafkaconsumerconcurrency.service.RetryService
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
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class RetryDltIntegrationTest {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, OrderMessage>

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var failureSimulator: FailureSimulator

    @Autowired
    private lateinit var dltConsumer: DltConsumer

    @BeforeEach
    fun setUp() {
        orderRepository.deleteAll()
        failureSimulator.clear()
        dltConsumer.clearDltMessages()
    }

    @Test
    fun `should send failed message to retry topic and eventually to DLT after max retries`() {
        val orderId = "failing-order-${System.currentTimeMillis()}"

        failureSimulator.addFailingOrderId(orderId)

        val message = OrderMessage(
            orderId = orderId,
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, orderId, message)).get()

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                assertTrue(dltConsumer.dltMessages.isNotEmpty(), "Should have at least one DLT message")
                val dltMessage = dltConsumer.dltMessages.find { it.key() == orderId }
                assertNotNull(dltMessage, "DLT should contain the failing order")
            }

        val dltMessage = dltConsumer.dltMessages.find { it.key() == orderId }!!
        assertEquals(orderId, dltMessage.value().orderId)

        assertTrue(
            failureSimulator.getFailureCount(orderId) >= RetryService.MAX_RETRY_COUNT + 1,
            "Should have attempted at least ${RetryService.MAX_RETRY_COUNT + 1} times"
        )

        val order = orderRepository.findById(orderId)
        assertTrue(order.isEmpty, "Order should not be saved due to failures")
    }

    @Test
    fun `should successfully process message after retry when failure is cleared`() {
        val orderId = "recoverable-order-${System.currentTimeMillis()}"

        failureSimulator.addFailingOrderId(orderId)

        val message = OrderMessage(
            orderId = orderId,
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, orderId, message)).get()

        Thread.sleep(2000)

        failureSimulator.removeFailingOrderId(orderId)

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val order = orderRepository.findById(orderId)
                assertTrue(order.isPresent, "Order should be saved after recovery")
            }

        val order = orderRepository.findById(orderId).get()
        assertEquals(orderId, order.orderId)
        assertEquals(100, order.amount)
    }

    @Test
    fun `should process successful messages while failing messages go to retry`() {
        val baseTime = System.currentTimeMillis()
        val successfulOrderId = "success-order-$baseTime"
        val failingOrderId = "fail-order-$baseTime"

        failureSimulator.addFailingOrderId(failingOrderId)

        val successMessage = OrderMessage(
            orderId = successfulOrderId,
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        val failMessage = OrderMessage(
            orderId = failingOrderId,
            customerId = "customer2",
            action = OrderAction.CREATE,
            amount = 200,
            timestamp = Instant.now()
        )

        kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, successfulOrderId, successMessage)).get()
        kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, failingOrderId, failMessage)).get()

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val successOrder = orderRepository.findById(successfulOrderId)
                assertTrue(successOrder.isPresent, "Successful order should be saved")
            }

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val dltMessage = dltConsumer.dltMessages.find { it.key() == failingOrderId }
                assertNotNull(dltMessage, "Failing order should end up in DLT")
            }

        val successOrder = orderRepository.findById(successfulOrderId).get()
        assertEquals(100, successOrder.amount)

        val failOrder = orderRepository.findById(failingOrderId)
        assertTrue(failOrder.isEmpty, "Failing order should not be saved")
    }

    @Test
    fun `should preserve message headers through retry process`() {
        val orderId = "header-test-order-${System.currentTimeMillis()}"

        failureSimulator.addFailingOrderId(orderId)

        val message = OrderMessage(
            orderId = orderId,
            customerId = "customer1",
            action = OrderAction.CREATE,
            amount = 100,
            timestamp = Instant.now()
        )

        kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, orderId, message)).get()

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                val dltMessage = dltConsumer.dltMessages.find { it.key() == orderId }
                assertNotNull(dltMessage, "Message should end up in DLT")
            }

        val dltMessage = dltConsumer.dltMessages.find { it.key() == orderId }!!

        val retryCountHeader = dltMessage.headers().lastHeader(RetryService.HEADER_RETRY_COUNT)
        assertNotNull(retryCountHeader, "Should have retry count header")

        val originalTopicHeader = dltMessage.headers().lastHeader(RetryService.HEADER_ORIGINAL_TOPIC)
        assertNotNull(originalTopicHeader, "Should have original topic header")
        assertEquals(KafkaConfig.MAIN_TOPIC, String(originalTopicHeader.value()))

        val exceptionHeader = dltMessage.headers().lastHeader(RetryService.HEADER_EXCEPTION)
        assertNotNull(exceptionHeader, "Should have exception header")
        assertTrue(String(exceptionHeader.value()).contains("Simulated failure"))
    }

    @Test
    fun `should handle multiple failing messages independently`() {
        val baseTime = System.currentTimeMillis()
        val failingOrderIds = (1..3).map { "multi-fail-$baseTime-$it" }

        failingOrderIds.forEach { failureSimulator.addFailingOrderId(it) }

        failingOrderIds.forEach { orderId ->
            val message = OrderMessage(
                orderId = orderId,
                customerId = "customer1",
                action = OrderAction.CREATE,
                amount = 100,
                timestamp = Instant.now()
            )
            kafkaTemplate.send(ProducerRecord(KafkaConfig.MAIN_TOPIC, orderId, message)).get()
        }

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted {
                failingOrderIds.forEach { orderId ->
                    val dltMessage = dltConsumer.dltMessages.find { it.key() == orderId }
                    assertNotNull(dltMessage, "Order $orderId should be in DLT")
                }
            }

        assertEquals(3, dltConsumer.dltMessages.filter { it.key()?.startsWith("multi-fail-$baseTime") == true }.size)
    }
}
