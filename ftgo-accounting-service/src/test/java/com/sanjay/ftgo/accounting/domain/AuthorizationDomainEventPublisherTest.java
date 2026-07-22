package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AuthorizationDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final AuthorizationDomainEventPublisher publisher =
            new AuthorizationDomainEventPublisher(outboxEventRepository, new ObjectMapper());

    @Test
    void publishesCardAuthorized() {
        publisher.publish(List.of(new CardAuthorizedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "CardAuthorized".equals(row.getEventType())
                        && "accounting.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesCardAuthorizationDeclinedWithReason() {
        publisher.publish(List.of(new CardAuthorizationDeclinedEvent(42L, "order quantity exceeds authorization limit")));

        verify(outboxEventRepository).save(argThat(row ->
                "CardAuthorizationFailed".equals(row.getEventType())
                        && row.getPayload().contains("order quantity exceeds authorization limit")));
    }

    @Test
    void publishesAuthorizationReversed() {
        publisher.publish(List.of(new AuthorizationReversedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationReversed".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesAuthorizationRevised() {
        publisher.publish(List.of(new AuthorizationRevisedEvent(42L, 8)));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationRevised".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesAuthorizationRevisionRejectedWithReason() {
        publisher.publish(List.of(new AuthorizationRevisionRejectedEvent(42L, "order quantity exceeds authorization limit")));

        verify(outboxEventRepository).save(argThat(row ->
                "AuthorizationRevisionRejected".equals(row.getEventType())
                        && row.getPayload().contains("order quantity exceeds authorization limit")));
    }
}
