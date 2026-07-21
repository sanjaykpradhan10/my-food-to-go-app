package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TicketDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final TicketDomainEventPublisher publisher =
            new TicketDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesTicketCreatedWithTotalQuantityAndTicketId() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketCreatedEvent(42L, 3)));

        verify(outboxEventRepository).save(argThat((OutboxEvent row) ->
                "TicketCreated".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"totalQuantity\":3")
                        && !row.getPayload().contains("readyBy")));
    }

    @Test
    void publishesTicketAcceptedWithoutLeakingReadyByOntoTheWire() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();
        ZonedDateTime readyBy = ZonedDateTime.parse("2026-07-20T18:00:00Z");

        publisher.publish(ticket, List.of(new TicketAcceptedEvent(42L, readyBy)));

        verify(outboxEventRepository).save(argThat((OutboxEvent row) ->
                "TicketAccepted".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && !row.getPayload().contains("readyBy")));
    }

    @Test
    void publishesTicketCreationFailedWithoutATicketInstance() {
        publisher.publishCreationFailed(new TicketCreationFailedEvent(43L, "order exceeds kitchen capacity"));

        verify(outboxEventRepository).save(argThat((OutboxEvent row) ->
                "TicketCreationFailed".equals(row.getEventType())
                        && row.getAggregateId().equals(43L)
                        && row.getPayload().contains("order exceeds kitchen capacity")));
    }

    @Test
    void publishesTicketCancellationRejectedWithReason() {
        Ticket ticket = Ticket.createTicket(42L, 3).ticket();

        publisher.publish(ticket, List.of(new TicketCancellationRejectedEvent(42L, "cannot cancel once ready for pickup")));

        verify(outboxEventRepository).save(argThat(row ->
                "TicketCancellationRejected".equals(row.getEventType())
                        && "kitchen.events".equals(row.getTopic())
                        && row.getPayload().contains("cannot cancel once ready for pickup")));
    }
}
