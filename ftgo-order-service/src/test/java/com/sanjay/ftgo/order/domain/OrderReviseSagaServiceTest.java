package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderReviseSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final OrderReviseSagaService service =
            new OrderReviseSagaService(orderRepository, processedEventRepository, domainEventPublisher);

    private Order revisionPendingOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        order.revise(new OrderRevision(List.of(new OrderLineItem(10L, 8))));
        return order;
    }

    @Test
    void confirmRevisionMovesOrderToApprovedWithRevisedLineItems() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmRevision(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 8));
        verify(domainEventPublisher).publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 8)))));
    }

    @Test
    void rejectRevisionMovesOrderBackToApprovedWithOriginalLineItems() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e2")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.rejectRevision(42L, "e2");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void compensateRevisionPublishesCompensationRequestWithoutChangingOrderStatus() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e3")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.compensateRevision(42L, "e3");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);
        verify(domainEventPublisher).publishRevisionCompensationRequested(any(Order.class), any());
        verify(orderRepository, never()).save(any());
    }

    @Test
    void finalizeRejectedRevisionMovesOrderBackToApproved() {
        Order order = revisionPendingOrder();
        when(processedEventRepository.existsById("e4")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.finalizeRejectedRevision(42L, "e4");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void skipsDuplicateConfirmRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.confirmRevision(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }

    @Test
    void ignoresConfirmRevisionForAnAlreadyResolvedOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmRevision(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository, never()).save(any());
    }
}
