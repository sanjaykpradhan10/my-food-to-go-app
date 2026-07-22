package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestrationOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    private final ReviseOrderSagaOrchestrator orchestrator;

    public OrchestrationOrderRevisionSagaTrigger(ReviseOrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
        orchestrator.start(order);
    }
}
