package com.d0lim.examplekafkaconsumerconcurrency.config

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaConfig(
    private val kafkaProperties: KafkaProperties
) {
    companion object {
        const val MAIN_TOPIC = "order-topic"
        const val RETRY_TOPIC = "order-topic-retry"
        const val DLT_TOPIC = "order-topic-dlt"
        const val PARTITION_COUNT = 3
        const val BATCH_SIZE = 500
    }

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers
        )
        return KafkaAdmin(configs)
    }

    @Bean
    fun mainTopic(): NewTopic = TopicBuilder
        .name(MAIN_TOPIC)
        .partitions(PARTITION_COUNT)
        .replicas(1)
        .build()

    @Bean
    fun retryTopic(): NewTopic = TopicBuilder
        .name(RETRY_TOPIC)
        .partitions(PARTITION_COUNT)
        .replicas(1)
        .build()

    @Bean
    fun dltTopic(): NewTopic = TopicBuilder
        .name(DLT_TOPIC)
        .partitions(PARTITION_COUNT)
        .replicas(1)
        .build()

    @Bean
    fun consumerFactory(): ConsumerFactory<String, OrderMessage> {
        val props = mutableMapOf<String, Any>()
        props.putAll(kafkaProperties.buildConsumerProperties())
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonDeserializer::class.java
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = BATCH_SIZE
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        props[JsonDeserializer.TRUSTED_PACKAGES] = "*"
        props[JsonDeserializer.VALUE_DEFAULT_TYPE] = OrderMessage::class.java.name
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun batchContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderMessage>
    ): ConcurrentKafkaListenerContainerFactory<String, OrderMessage> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, OrderMessage>()
        factory.setConsumerFactory(consumerFactory)
        factory.setBatchListener(true)
        factory.setConcurrency(PARTITION_COUNT)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        return factory
    }

    @Bean
    fun retryContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderMessage>
    ): ConcurrentKafkaListenerContainerFactory<String, OrderMessage> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, OrderMessage>()
        factory.setConsumerFactory(consumerFactory)
        factory.setBatchListener(false)
        factory.setConcurrency(3)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        return factory
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, OrderMessage> {
        val props = mutableMapOf<String, Any>()
        props.putAll(kafkaProperties.buildProducerProperties())
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, OrderMessage>): KafkaTemplate<String, OrderMessage> {
        return KafkaTemplate(producerFactory)
    }
}
