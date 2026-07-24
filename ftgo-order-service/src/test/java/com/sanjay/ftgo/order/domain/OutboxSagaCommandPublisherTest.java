package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxSagaCommandPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Test
    void publishSavesAnOutboxEventWithTheGivenTopic() {
        OutboxSagaCommandPublisher publisher = new OutboxSagaCommandPublisher(outboxEventRepository, new ObjectMapper());

        publisher.publish("kitchen.commands", "evt-1", "CreateTicket", 42L,
                new KitchenCommand("evt-1", "CreateTicket", 42L, 3, "CreateOrder"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getEventId()).isEqualTo("evt-1");
        assertThat(captor.getValue().getEventType()).isEqualTo("CreateTicket");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("kitchen.commands");
        assertThat(captor.getValue().getPayload()).contains("\"commandType\":\"CreateTicket\"");
    }
}
