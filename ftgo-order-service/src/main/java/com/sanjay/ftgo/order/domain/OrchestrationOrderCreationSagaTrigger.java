package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final CreateOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderCreationSagaTrigger(CreateOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        orchestrator.start(order);
    }
}
