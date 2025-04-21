package com.oms.notificationservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.notificationservice.dto.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@EmbeddedKafka(partitions = 1, controlledShutdown = true)
public class KafkaConsumerServiceTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerServiceTest.class);

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Spy // Use Spy to partially mock the real KafkaConsumerService
    @InjectMocks
    private KafkaConsumerService kafkaConsumerService;

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
    }

    @Test
    void testConsumeOrderEvent() throws Exception {
        // Arrange: Create an OrderEvent
        OrderEvent orderEvent = new OrderEvent("1", "CREATED", "Order has been created");

        // Serialize the OrderEvent into JSON
        String orderEventJson = objectMapper.writeValueAsString(orderEvent);

        // Create a producer record to simulate a Kafka message
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("order-events", orderEventJson);

        // Act: Send the message to the embedded Kafka broker
        kafkaTemplate.send(producerRecord);

        // Simulate consuming the message
        kafkaConsumerService.consume(orderEvent);

        // Assert: Verify that the consume method was called exactly once
        verify(kafkaConsumerService, times(1)).consume(orderEvent);
    }

}
