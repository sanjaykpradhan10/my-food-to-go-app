package com.sanjay.ftgo.order.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@Service
@ConditionalOnProperty(name = "persistence.mode", havingValue = "jpa", matchIfMissing = true)
public class JpaOrderTransitions implements OrderTransitions {

    private static final Logger log = LoggerFactory.getLogger(JpaOrderTransitions.class);

    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public JpaOrderTransitions(OrderRepository orderRepository, OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    @Transactional
    public Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId) {
        return orderRepository.save(new Order(consumerId, restaurantId, lineItems, OrderStatus.APPROVAL_PENDING));
    }

    @Override
    public java.util.Optional<Order> findById(Long orderId) {
        return orderRepository.findById(orderId);
    }

    @Override
    @Transactional
    public TransitionResult cancel(Long orderId, String eventId) {
        Order order = findOrThrow(orderId);
        List<OrderDomainEvent> events = order.cancel();
        orderRepository.save(order);
        return new TransitionResult(order, events);
    }

    @Override
    @Transactional
    public TransitionResult revise(Long orderId, OrderRevision revision, String eventId) {
        Order order = findOrThrow(orderId);
        List<OrderDomainEvent> events = order.revise(revision);
        orderRepository.save(order);
        return new TransitionResult(order, events);
    }

    @Override
    @Transactional
    public void approve(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteApproved, "approve");
    }

    @Override
    @Transactional
    public void reject(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteRejected, "reject");
    }

    @Override
    @Transactional
    public void noteCancelled(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::noteCancelled, "cancel confirmation");
    }

    @Override
    @Transactional
    public void undoCancel(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::undoCancel, "cancel rejection");
    }

    @Override
    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::confirmRevision, "revision confirmation");
    }

    @Override
    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, Order::rejectRevision, "revision rejection");
    }

    @Override
    @Transactional
    public void requestRevisionCompensation(Long orderId, String eventId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.REVISION_PENDING) {
            return;
        }
        // A fresh id, not the inbound eventId - matches the pre-existing behavior this facade
        // extracts (OrderReviseSagaService.compensateRevision), which mints its own id for this
        // outbound pseudo-event since it's a distinct identity from whatever triggered it.
        String compensationEventId = UUID.randomUUID().toString();
        domainEventPublisher.publishRevisionCompensationRequested(order, compensationEventId);
    }

    private void applyBestEffort(Long orderId, Function<Order, List<OrderDomainEvent>> transition, String description) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = transition.apply(order);
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring {} for order {}: {}", description, orderId, e.getMessage());
        }
    }

    private Order findOrThrow(Long orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
