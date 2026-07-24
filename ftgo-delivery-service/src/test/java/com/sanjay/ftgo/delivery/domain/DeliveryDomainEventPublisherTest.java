package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeliveryDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final DeliveryDomainEventPublisher publisher =
            new DeliveryDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesDeliveryScheduledToDeliveryEventsTopic() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        publisher.publish(delivery, List.of(new DeliveryScheduledEvent(42L, 3L)));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("DeliveryScheduled");
        assertThat(saved.getAggregateId()).isEqualTo(42L);
        assertThat(saved.getTopic()).isEqualTo("delivery.events");
        assertThat(saved.getPayload()).contains("\"courierId\":3");
    }

    @Test
    void publishesSchedulingFailedWithNoDeliveryEntity() {
        publisher.publishSchedulingFailed(new DeliverySchedulingFailedEvent(42L, "no courier available"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("DeliverySchedulingFailed");
        assertThat(saved.getPayload()).contains("\"reason\":\"no courier available\"");
    }
}
