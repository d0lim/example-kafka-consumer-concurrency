package com.d0lim.examplekafkaconsumerconcurrency

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.utility.DockerImageName

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    companion object {
        @JvmStatic
        val kafkaContainer: KafkaContainer = KafkaContainer(
            DockerImageName.parse("apache/kafka-native:latest")
        ).apply {
            start()
        }

        @JvmStatic
        val mongoDBContainer: MongoDBContainer = MongoDBContainer(
            DockerImageName.parse("mongo:latest")
        ).apply {
            start()
        }
    }

    @Bean
    @ServiceConnection
    fun kafkaContainer(): KafkaContainer = Companion.kafkaContainer

    @Bean
    @ServiceConnection
    fun mongoDbContainer(): MongoDBContainer = Companion.mongoDBContainer
}
