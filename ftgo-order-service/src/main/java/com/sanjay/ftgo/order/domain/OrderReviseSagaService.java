package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderReviseSagaService {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;

    public OrderReviseSagaService(OrderTransitions orderTransitions, ProcessedEventRepository processedEventRepository) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void confirmRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.confirmRevision(orderId, eventId);
    }

    @Transactional
    public void rejectRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.rejectRevision(orderId, eventId);
    }

    // Triggers kitchen's undo without changing Order's own status - Order must stay
    // REVISION_PENDING until finalizeRejectedRevision runs, once the undo is confirmed.
    @Transactional
    public void compensateRevision(Long orderId, String eventId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));
        orderTransitions.requestRevisionCompensation(orderId, eventId);
    }

    @Transactional
    public void finalizeRejectedRevision(Long orderId, String eventId) {
        rejectRevision(orderId, eventId);
    }
}
