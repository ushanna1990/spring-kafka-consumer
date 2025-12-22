package com.kafka.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.model.Order;
import com.kafka.consumer.model.Payment;
import com.kafka.consumer.service.KafkaConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaMessageConsumer {

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    @KafkaListener(topics = "order-topic", groupId = "consumer-group-id")
    public void consumeOrderData(String message) throws JsonProcessingException {
        List<Order> orders = new ObjectMapper().readValue(message, new TypeReference<>() {});
        kafkaConsumerService.consumeOrderData(orders);
    }

    @KafkaListener(topics = "payment-topic", groupId = "consumer-group-id")
    public void consumePaymentData(String message) throws JsonProcessingException {
        List<Payment> payments = new ObjectMapper().readValue(message, new TypeReference<>() {});
        kafkaConsumerService.consumePaymentData(payments);
    }
}
