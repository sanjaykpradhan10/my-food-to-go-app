package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderCancelSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderCancelSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void confirmCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.noteCancelled(orderId, eventId);
    }

    @Transactional
    public void rejectCancel(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.undoCancel(orderId, eventId);
    }
}
