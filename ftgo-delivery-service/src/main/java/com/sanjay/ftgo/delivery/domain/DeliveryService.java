package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final CourierRepository courierRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DeliveryDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public DeliveryService(DeliveryRepository deliveryRepository,
                            CourierRepository courierRepository,
                            ProcessedEventRepository processedEventRepository,
                            FailedOrderRepository failedOrderRepository,
                            OutboxEventRepository outboxEventRepository,
                            DeliveryDomainEventPublisher domainEventPublisher,
                            ObjectMapper objectMapper) {
        this.deliveryRepository = deliveryRepository;
        this.courierRepository = courierRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    // Choreography: delivery-service's own parallel-join leg, triggered directly by OrderCreated
    // (not gated on the other two legs, exactly like kitchen's ticket creation).
    @Transactional
    public void handleOrderCreated(String eventId, Long orderId, Long restaurantId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (failedOrderRepository.existsById(orderId)) {
            return;
        }

        schedule(orderId, restaurantId);
    }

    // Orchestration equivalent of handleOrderCreated: replies on saga.replies instead of
    // broadcasting on delivery.events, since the orchestrator (not a peer listener) is waiting.
    @Transactional
    public void handleScheduleDeliveryCommand(String eventId, Long orderId, Long restaurantId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Optional<Courier> available = courierRepository.findFirstByAvailableTrue();
        if (available.isEmpty()) {
            publishReply("DeliverySchedulingFailed", orderId, "no courier available", "CreateOrder");
            return;
        }
        Courier courier = available.get();
        courier.setAvailable(false);
        courierRepository.save(courier);

        DeliveryScheduleResult result = Delivery.schedule(orderId, restaurantId, courier.getId());
        deliveryRepository.save(result.delivery());
        publishReply("DeliveryScheduled", orderId, null, "CreateOrder");
    }

    private void schedule(Long orderId, Long restaurantId) {
        Optional<Courier> available = courierRepository.findFirstByAvailableTrue();
        if (available.isEmpty()) {
            domainEventPublisher.publishSchedulingFailed(new DeliverySchedulingFailedEvent(orderId, "no courier available"));
            return;
        }
        Courier courier = available.get();
        courier.setAvailable(false);
        courierRepository.save(courier);

        DeliveryScheduleResult result = Delivery.schedule(orderId, restaurantId, courier.getId());
        Delivery delivery = deliveryRepository.save(result.delivery());
        domainEventPublisher.publish(delivery, result.events());
    }

    // Choreography: single entry point for every release trigger - a real Cancel Order request
    // (kitchen's TicketCancelled) and every Create Order compensation path (ConsumerVerificationFailed,
    // TicketCreationFailed, CardAuthorizationFailed all funnel here). If the delivery hasn't been
    // scheduled yet (the compensation trigger raced ahead of this service's own OrderCreated
    // processing), record FailedOrder so the eventual schedule() call becomes a no-op instead of
    // assigning a courier to an order that's already doomed - same race this codebase already
    // solved for kitchen's Ticket.
    @Transactional
    public void release(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Delivery delivery = deliveryRepository.findByOrderId(orderId).orElse(null);
        if (delivery == null) {
            failedOrderRepository.save(new FailedOrder(orderId));
            return;
        }
        List<DeliveryDomainEvent> events = delivery.cancel();
        deliveryRepository.save(delivery);
        releaseCourier(delivery);
        domainEventPublisher.publish(delivery, events);
    }

    // Orchestration equivalent of release: replies on saga.replies. Unconditional once the
    // orchestrator sends this command (spec decision 5 - no decline path for releasing a
    // courier), so no FailedOrder handling is needed here: the orchestrator only ever sends
    // ReleaseDelivery after it already has confirmation the delivery was scheduled.
    @Transactional
    public void handleReleaseDeliveryCommand(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Delivery delivery = deliveryRepository.findByOrderId(orderId).orElse(null);
        if (delivery == null) {
            return;
        }
        delivery.cancel();
        deliveryRepository.save(delivery);
        releaseCourier(delivery);
        publishReply("DeliveryCancelled", orderId, null, sagaType);
    }

    private void releaseCourier(Delivery delivery) {
        courierRepository.findById(delivery.getCourierId()).ifPresent(courier -> {
            courier.setAvailable(true);
            courierRepository.save(courier);
        });
    }

    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "delivery", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga event", e);
        }
    }
}
