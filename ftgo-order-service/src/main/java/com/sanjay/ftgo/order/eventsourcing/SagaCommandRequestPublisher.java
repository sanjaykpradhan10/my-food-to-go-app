package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

// Second half of the book's §6.3.4 two-step pseudo-event mechanism: Task 19's
// EventSourcedSagaCommandPublisher durably records the intent to send a saga command in the same
// transaction as the Order's event append; this poller later reads those unpublished rows and
// actually sends them to Kafka, mirroring ftgo-common's OutboxPublisher but scoped to
// order_saga_command_requests and active only in event-sourcing persistence mode.
//
// publishPending() is invoked on a schedule by SagaCommandRequestSchedulingConfig's Trigger
// (not a plain @Scheduled here) so the poll interval, backed by the @RefreshScope
// SagaCommandRequestProperties bean, can be changed live via POST /actuator/refresh — a plain
// @Scheduled placeholder would only resolve once, at bean-creation time.
@Component
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class SagaCommandRequestPublisher {

    private final OrderSagaCommandRequestRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SagaCommandRequestPublisher(OrderSagaCommandRequestRepository repository,
                                        KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPending() {
        List<OrderSagaCommandRequest> pending = repository.findByPublishedAtIsNullOrderByIdAsc();
        for (OrderSagaCommandRequest request : pending) {
            kafkaTemplate.send(request.getTargetTopic(), request.getOrderId().toString(), request.getPayload());
            request.markPublished();
            repository.save(request);
        }
    }
}
