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

    // The contract's example input message (Task 6, Step 3) — verifying that this consumer's
    // publish() call produces exactly this JSON shape confirms it stays compatible with what
    // KitchenCommandListener (the real provider, per the shared contract) expects to receive.
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
