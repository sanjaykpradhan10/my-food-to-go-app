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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OrderCancelSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);
    private final OrderCancelSagaService service =
            new OrderCancelSagaService(orderRepository, processedEventRepository, domainEventPublisher);

    private Order cancelPendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
    }

    @Test
    void confirmCancelMovesOrderToCancelled() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderCancelConfirmedEvent(42L)));
    }

    @Test
    void rejectCancelMovesOrderBackToApproved() {
        Order order = cancelPendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.rejectCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository).save(order);
        verify(domainEventPublisher).publish(List.of(new OrderCancelRejectedEvent(42L)));
    }

    @Test
    void ignoresConfirmCancelForAnAlreadyResolvedOrder() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCELLED);
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        service.confirmCancel(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderRepository, never()).save(any());
        verifyNoInteractions(domainEventPublisher);
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        service.confirmCancel(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
