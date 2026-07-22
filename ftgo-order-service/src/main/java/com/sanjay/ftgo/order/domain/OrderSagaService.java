package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void approve(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.approve(orderId, eventId);
    }

    @Transactional
    public void reject(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.reject(orderId, eventId);
    }
}
