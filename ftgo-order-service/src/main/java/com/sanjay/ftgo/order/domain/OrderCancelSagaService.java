package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OrderCancelSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelSagaService.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderCancelSagaService(OrderRepository orderRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   OrderDomainEventPublisher domainEventPublisher) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void confirmCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteCancelled();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    @Transactional
    public void rejectCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.undoCancel();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel rejection for order {}: {}", orderId, e.getMessage());
        }
    }
}
