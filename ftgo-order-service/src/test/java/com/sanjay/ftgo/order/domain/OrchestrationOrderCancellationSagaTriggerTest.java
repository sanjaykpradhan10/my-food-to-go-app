package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrchestrationOrderCancellationSagaTriggerTest {

    private final CancelOrderSagaOrchestrator orchestrator = mock(CancelOrderSagaOrchestrator.class);

    private final OrchestrationOrderCancellationSagaTrigger trigger =
            new OrchestrationOrderCancellationSagaTrigger(orchestrator);

    @Test
    void startsTheOrchestratorWithoutPublishingEventsDirectly() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);

        trigger.onOrderCancelled(order, List.of(new OrderCancelledEvent(1L)));

        verify(orchestrator).start(order);
    }
}
