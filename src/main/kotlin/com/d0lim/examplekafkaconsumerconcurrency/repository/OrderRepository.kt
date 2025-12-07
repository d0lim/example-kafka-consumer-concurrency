package com.d0lim.examplekafkaconsumerconcurrency.repository

import com.d0lim.examplekafkaconsumerconcurrency.domain.OrderEntity
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface OrderRepository : MongoRepository<OrderEntity, String>
