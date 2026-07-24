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
 * Scope is intentionally narrow: this class only verifies OrderCancelSagaService's own
 * responsibility (the idempotency gate and delegation to OrderTransitions). Order
 * state-transition behavior now lives in OrderAggregateTest and JpaOrderTransitionsTest.
 */
class OrderCancelSagaServiceTest {

    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderCancelSagaService orderCancelSagaService =
            new OrderCancelSagaService(orderTransitions, processedEventRepository);

    @Test
    void delegatesConfirmCancelToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderCancelSagaService.confirmCancel(42L, "e1");

        verify(orderTransitions).noteCancelled(42L, "e1");
    }

    @Test
    void delegatesRejectCancelToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderCancelSagaService.rejectCancel(42L, "e1");

        verify(orderTransitions).undoCancel(42L, "e1");
    }

    @Test
    void skipsDuplicateConfirmCancelDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderCancelSagaService.confirmCancel(42L, "e1");

        verify(orderTransitions, never()).noteCancelled(anyLong(), any());
    }

    @Test
    void skipsDuplicateRejectCancelDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderCancelSagaService.rejectCancel(42L, "e1");

        verify(orderTransitions, never()).undoCancel(anyLong(), any());
    }
}
