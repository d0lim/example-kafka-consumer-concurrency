package com.d0lim.examplekafkaconsumerconcurrency.service

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component

data class PartitionedMessages(
    val uniqueKeyMessages: List<ConsumerRecord<String, OrderMessage>>,
    val duplicateKeyGroups: Map<String, List<ConsumerRecord<String, OrderMessage>>>
)

@Component
class MessagePartitioner {

    fun partition(messages: List<ConsumerRecord<String, OrderMessage>>): PartitionedMessages {
        val keyCount = messages.groupingBy { it.key() }.eachCount()

        val uniqueKeyMessages = messages.filter { keyCount[it.key()] == 1 }
        val duplicateKeyGroups = messages
            .filter { keyCount[it.key()]!! > 1 }
            .groupBy { it.key() }

        return PartitionedMessages(
            uniqueKeyMessages = uniqueKeyMessages,
            duplicateKeyGroups = duplicateKeyGroups
        )
    }
}
