package com.d0lim.examplekafkaconsumerconcurrency

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ExampleKafkaConsumerConcurrencyApplication

fun main(args: Array<String>) {
    runApplication<ExampleKafkaConsumerConcurrencyApplication>(*args)
}
