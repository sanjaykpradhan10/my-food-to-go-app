package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class OrderReviseSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderReviseSagaService.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderReviseSagaService(OrderRepository orderRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.confirmRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.rejectRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision rejection for order {}: {}", orderId, e.getMessage());
        }
    }

    // Triggers kitchen's undo without changing Order's own status - Order must stay
    // REVISION_PENDING until finalizeRejectedRevision runs, once the undo is confirmed.
    @Transactional
    public void compensateRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.REVISION_PENDING) {
            return;
        }
        String compensationEventId = UUID.randomUUID().toString();
        domainEventPublisher.publishRevisionCompensationRequested(order, compensationEventId);
    }

    @Transactional
    public void finalizeRejectedRevision(Long orderId, String eventId) {
        rejectRevision(orderId, eventId);
    }
}
