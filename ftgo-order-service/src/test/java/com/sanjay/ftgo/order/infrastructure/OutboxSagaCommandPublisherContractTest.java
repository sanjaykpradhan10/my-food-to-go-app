package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.order.domain.KitchenCommand;
import com.sanjay.ftgo.order.domain.OutboxSagaCommandPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OutboxSagaCommandPublisherContractTest {

    // The command values and expected JSON below are a hand-copied mirror of the input message
    // in ftgo-kitchen-service-contracts/src/main/resources/contracts/kitchen/shouldCreateTicket.groovy
    // (Task 6) — NOT resolved through Stub Runner (see ftgo-kitchen-service-contracts/build.gradle
    // for why this module publishes no stub jar). Because this is a manual copy, it does not
    // automatically track changes to the contract file: if shouldCreateTicket.groovy's input
    // fields or example values change, this literal must be updated too.
    @Test
    void publishesCreateTicketCommandMatchingTheContract() {
        OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
        OutboxSagaCommandPublisher publisher = new OutboxSagaCommandPublisher(outboxEventRepository, new ObjectMapper());

        KitchenCommand command = new KitchenCommand(
                "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, 2, "CreateOrder");
        publisher.publish("kitchen.commands", "22222222-2222-2222-2222-222222222222", "CreateTicket", 1223232L, command);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertEquals("kitchen.commands", captor.getValue().getTopic());
        assertEquals(
                "{\"eventId\":\"22222222-2222-2222-2222-222222222222\",\"commandType\":\"CreateTicket\","
                        + "\"orderId\":1223232,\"totalQuantity\":2,\"sagaType\":\"CreateOrder\"}",
                captor.getValue().getPayload());
    }
}
