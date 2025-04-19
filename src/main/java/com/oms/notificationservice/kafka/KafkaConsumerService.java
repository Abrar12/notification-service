package com.oms.notificationservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.notificationservice.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void consume(OrderEvent orderEvent) {
        try {
            log.info("🔔 Notification received for Order: {}, Status: {}, Message: {}",
                    orderEvent.getOrderId(), orderEvent.getStatus(), orderEvent.getMessage());
        } catch (Exception e) {
            log.error("❌ Failed to deserialize message: {}", orderEvent, e);
        }
    }
}
