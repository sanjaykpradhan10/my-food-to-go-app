package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderDomainEventPublisherTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final OrderDomainEventPublisher publisher =
            new OrderDomainEventPublisher(outboxEventRepository, new OrderEventSerializer(new ObjectMapper()));

    @Test
    void publishOrderCreatedKeepsTheSameWireShapeAsBeforeTheRefactor() {
        Order order = new Order(42L, 7L, 3L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        publisher.publishOrderCreated(order, "event-1");

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCreated".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"eventId\":\"event-1\"")
                        && row.getPayload().contains("\"eventType\":\"OrderCreated\"")
                        && row.getPayload().contains("\"orderId\":42")
                        && row.getPayload().contains("\"consumerId\":7")
                        && row.getPayload().contains("\"restaurantId\":3")
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":2")));
    }

    @Test
    void publishesOrderApprovedWithoutConsumerOrLineItemFields() {
        publisher.publish(List.of(new OrderApprovedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderApproved".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"orderId\":42")
                        && row.getPayload().contains("\"consumerId\":null")
                        && row.getPayload().contains("\"lineItems\":null")));
    }

    @Test
    void publishesOrderRejected() {
        publisher.publish(List.of(new OrderRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelledProposal() {
        publisher.publish(List.of(new OrderCancelledEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelled".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelConfirmed() {
        publisher.publish(List.of(new OrderCancelConfirmedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelConfirmed".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderCancelRejected() {
        publisher.publish(List.of(new OrderCancelRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderCancelRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesOrderRevisionProposedWithRevisedLineItems() {
        publisher.publish(List.of(new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5)))));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionProposed".equals(row.getEventType())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":5")));
    }

    @Test
    void publishesOrderRevisedWithRevisedLineItems() {
        publisher.publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 5)))));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevised".equals(row.getEventType())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"quantity\":5")));
    }

    @Test
    void publishesOrderRevisionRejected() {
        publisher.publish(List.of(new OrderRevisionRejectedEvent(42L)));

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionRejected".equals(row.getEventType()) && row.getAggregateId().equals(42L)));
    }

    @Test
    void publishesRevisionCompensationRequestedWithOriginalLineItems() {
        Order order = new Order(42L, 7L, 3L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING);

        publisher.publishRevisionCompensationRequested(order, "event-9");

        verify(outboxEventRepository).save(argThat(row ->
                "OrderRevisionCompensationRequested".equals(row.getEventType())
                        && "order.events".equals(row.getTopic())
                        && row.getAggregateId().equals(42L)
                        && row.getPayload().contains("\"eventId\":\"event-9\"")
                        && row.getPayload().contains("\"menuItemId\":10")
                        && row.getPayload().contains("\"quantity\":2")));
    }
}
