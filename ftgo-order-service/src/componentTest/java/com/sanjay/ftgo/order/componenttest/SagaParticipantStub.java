package com.sanjay.ftgo.order.componenttest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Stands in for the Consumer, Kitchen, Delivery, and Accounting services during the Place Order
 * component test. CreateOrderSagaOrchestrator fans out VerifyConsumerCommand/KitchenCommand
 * (CreateTicket)/DeliveryCommand (ScheduleDelivery) in parallel, then AccountingCommand
 * (AuthorizeCard) only once all three replies succeed — so this stub always replies success for
 * consumer/kitchen/delivery, and only the accounting reply is configurable per scenario.
 */
public class SagaParticipantStub implements AutoCloseable {

    private static final String CONSUMER_COMMANDS = "consumer.commands";
    private static final String KITCHEN_COMMANDS = "kitchen.commands";
    private static final String DELIVERY_COMMANDS = "delivery.commands";
    private static final String ACCOUNTING_COMMANDS = "accounting.commands";
    private static final String SAGA_REPLIES = "saga.replies";
    private static final String SAGA_TYPE = "CreateOrder";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;
    private volatile boolean accountingShouldApprove = true;

    public SagaParticipantStub(String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "saga-participant-stub-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(CONSUMER_COMMANDS, KITCHEN_COMMANDS, DELIVERY_COMMANDS, ACCOUNTING_COMMANDS));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        executor.submit(this::pollLoop);
    }

    public void setAccountingShouldApprove(boolean approve) {
        this.accountingShouldApprove = approve;
    }

    private void pollLoop() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    handleCommand(record.topic(), record.value());
                } catch (Exception e) {
                    // A malformed or not-yet-understood command shouldn't kill the poll loop —
                    // the test itself will time out and fail if a reply never arrives.
                }
            }
        }
    }

    private void handleCommand(String topic, String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        long orderId = node.get("orderId").asLong();
        switch (topic) {
            case CONSUMER_COMMANDS -> reply("consumer", "ConsumerVerified", orderId);
            case KITCHEN_COMMANDS -> {
                if ("CreateTicket".equals(node.get("commandType").asText())) {
                    reply("kitchen", "TicketCreated", orderId);
                }
            }
            case DELIVERY_COMMANDS -> {
                if ("ScheduleDelivery".equals(node.get("commandType").asText())) {
                    reply("delivery", "DeliveryScheduled", orderId);
                }
            }
            case ACCOUNTING_COMMANDS -> {
                if ("AuthorizeCard".equals(node.get("commandType").asText())) {
                    reply("accounting", accountingShouldApprove ? "CardAuthorized" : "CardAuthorizationFailed", orderId);
                }
            }
            default -> { }
        }
    }

    private void reply(String participant, String eventType, long orderId) {
        String eventId = UUID.randomUUID().toString();
        String json = String.format(
                "{\"eventId\":\"%s\",\"participant\":\"%s\",\"eventType\":\"%s\",\"orderId\":%d,\"reason\":null,\"sagaType\":\"%s\"}",
                eventId, participant, eventType, orderId, SAGA_TYPE);
        try {
            producer.send(new ProducerRecord<>(SAGA_REPLIES, String.valueOf(orderId), json)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish saga reply for order " + orderId, e);
        }
    }

    @Override
    public void close() {
        // KafkaConsumer is not thread-safe: closing it from this thread while pollLoop (running on
        // the executor) is still mid-poll() throws ConcurrentModificationException. Signal the loop
        // to stop and wait for it to actually exit before touching the consumer/producer here.
        running = false;
        executor.shutdown();
        try {
            executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
        producer.close();
    }
}
