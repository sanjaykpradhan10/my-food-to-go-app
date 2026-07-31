package com.sanjay.ftgo.kitchen.contracttest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.contracttest.KafkaContractTestSupport;
import com.sanjay.ftgo.common.outbox.OutboxPublisher;
import com.sanjay.ftgo.kitchen.domain.KitchenCommand;
import com.sanjay.ftgo.kitchen.infrastructure.KitchenCommandListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;

// @EmbeddedKafka must be repeated directly here rather than relying on
// KafkaContractTestSupport's own @EmbeddedKafka: Spring's TestContextAnnotationUtils only walks
// the test-class hierarchy, not classes reached via @Import (same issue Task 4's order-service
// MessagingBase hit and documents on KafkaContractTestSupport itself) - without it, the embedded
// broker bean this test needs is never created, and constructor autowiring fails with
// NoSuchBeanDefinitionException for EmbeddedKafkaBroker.
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"order.events", "kitchen.commands", "saga.replies"})
@AutoConfigureMessageVerifier
@Import(KafkaContractTestSupport.class)
public abstract class MessagingBase {

    @Autowired
    private KitchenCommandListener kitchenCommandListener;

    @Autowired
    private ObjectMapper objectMapper;

    // TicketService.handleCreateTicketCommand writes the TicketCreated reply to the outbox
    // rather than publishing straight to Kafka (same outbox pattern as order-service's
    // MessagingBase, Task 4) - the generated contract test never sees anything on saga.replies
    // without this explicit relay step.
    @Autowired
    private OutboxPublisher outboxPublisher;

    protected void createTicketCommandReceived() throws Exception {
        KitchenCommand command = new KitchenCommand(
                "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, 2, "CreateOrder");
        kitchenCommandListener.onMessage(objectMapper.writeValueAsString(command));
        outboxPublisher.publishPendingEvents();
    }
}
