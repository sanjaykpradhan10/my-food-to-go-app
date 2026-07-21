package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    private final CancelOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderCancellationSagaTrigger(CancelOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
        orchestrator.start(order);
    }
}
