package com.kafka.consumer.service;

import com.kafka.consumer.model.Order;
import com.kafka.consumer.model.Payment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaConsumerService {


    public void consumeOrderData(List<Order> orders) {
        orders.forEach(order -> System.out.println("Consumed Order: " + order));
        System.out.println("Orders data consumed from kafka topic successfully");
    }

    public void consumePaymentData(List<Payment>  payments) {
        payments.forEach(payment -> System.out.println("Consumed Payment: " + payment));
        System.out.println("Payments data consumed from kafka topic successfully");
    }
}
