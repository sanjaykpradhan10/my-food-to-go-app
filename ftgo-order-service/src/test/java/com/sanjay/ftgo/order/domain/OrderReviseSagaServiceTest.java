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
 * Scope is intentionally narrow: this class only verifies OrderReviseSagaService's own
 * responsibility (the idempotency gate and delegation to OrderTransitions). Order
 * state-transition behavior now lives in OrderAggregateTest and JpaOrderTransitionsTest.
 */
class OrderReviseSagaServiceTest {

    private final OrderTransitions orderTransitions = mock(OrderTransitions.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final OrderReviseSagaService orderReviseSagaService =
            new OrderReviseSagaService(orderTransitions, processedEventRepository);

    @Test
    void delegatesConfirmRevisionToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderReviseSagaService.confirmRevision(42L, "e1");

        verify(orderTransitions).confirmRevision(42L, "e1");
    }

    @Test
    void delegatesRejectRevisionToOrderTransitionsForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderReviseSagaService.rejectRevision(42L, "e1");

        verify(orderTransitions).rejectRevision(42L, "e1");
    }

    @Test
    void delegatesCompensateRevisionToRequestRevisionCompensationForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderReviseSagaService.compensateRevision(42L, "e1");

        verify(orderTransitions).requestRevisionCompensation(42L, "e1");
        verify(orderTransitions, never()).confirmRevision(anyLong(), any());
        verify(orderTransitions, never()).rejectRevision(anyLong(), any());
    }

    @Test
    void finalizeRejectedRevisionDelegatesToRejectRevisionForAFreshEvent() {
        when(processedEventRepository.existsById("e1")).thenReturn(false);

        orderReviseSagaService.finalizeRejectedRevision(42L, "e1");

        verify(orderTransitions).rejectRevision(42L, "e1");
    }

    @Test
    void skipsDuplicateConfirmRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderReviseSagaService.confirmRevision(42L, "e1");

        verify(orderTransitions, never()).confirmRevision(anyLong(), any());
    }

    @Test
    void skipsDuplicateRejectRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderReviseSagaService.rejectRevision(42L, "e1");

        verify(orderTransitions, never()).rejectRevision(anyLong(), any());
    }

    @Test
    void skipsDuplicateCompensateRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderReviseSagaService.compensateRevision(42L, "e1");

        verify(orderTransitions, never()).requestRevisionCompensation(anyLong(), any());
    }

    @Test
    void skipsDuplicateFinalizeRejectedRevisionDelivery() {
        when(processedEventRepository.existsById("e1")).thenReturn(true);

        orderReviseSagaService.finalizeRejectedRevision(42L, "e1");

        verify(orderTransitions, never()).rejectRevision(anyLong(), any());
    }
}
