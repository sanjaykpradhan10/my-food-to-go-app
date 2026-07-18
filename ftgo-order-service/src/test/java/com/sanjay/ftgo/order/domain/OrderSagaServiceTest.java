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

class OrderSagaServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderSagaService orderSagaService = new OrderSagaService(orderRepository, processedEventRepository);

    private Order pendingOrder() {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void approvesOrderInApprovalPending() {
        Order order = pendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.approve(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository).save(order);
    }

    @Test
    void rejectsOrderInApprovalPending() {
        Order order = pendingOrder();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.reject(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository).save(order);
    }

    @Test
    void doesNotReapproveAnAlreadyRejectedOrder() {
        Order order = pendingOrder();
        order.markRejected();
        when(processedEventRepository.existsById("e1")).thenReturn(false);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        orderSagaService.approve(42L, "e1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void skipsDuplicateDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderSagaService.approve(42L, "e1");

        verify(orderRepository, never()).findById(any());
    }
}
