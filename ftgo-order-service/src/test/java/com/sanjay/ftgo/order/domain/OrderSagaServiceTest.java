package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Scope is intentionally narrow: this class only verifies OrderSagaService's own
 * responsibility (the idempotency gate and delegation to OrderTransitions). Order
 * state-transition behavior now lives in OrderAggregateTest and JpaOrderTransitionsTest.
 */
class OrderSagaServiceTest {

    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderSagaService orderSagaService =
            new OrderSagaService(orderTransitions, processedEventRepository);

    @Test
    void delegatesApproveToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderSagaService.approve(42L, "e1");

        verify(orderTransitions).approve(42L, "e1");
    }

    @Test
    void delegatesRejectToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderSagaService.reject(42L, "e1");

        verify(orderTransitions).reject(42L, "e1");
    }

    @Test
    void skipsDuplicateApproveDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderSagaService.approve(42L, "e1");

        verify(orderTransitions, never()).approve(anyLong(), any());
    }

    @Test
    void skipsDuplicateRejectDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderSagaService.reject(42L, "e1");

        verify(orderTransitions, never()).reject(anyLong(), any());
    }
}
