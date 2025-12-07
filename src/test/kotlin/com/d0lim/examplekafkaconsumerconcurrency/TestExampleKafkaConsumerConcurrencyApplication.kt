package com.d0lim.examplekafkaconsumerconcurrency

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<ExampleKafkaConsumerConcurrencyApplication>().with(TestcontainersConfiguration::class).run(*args)
}
