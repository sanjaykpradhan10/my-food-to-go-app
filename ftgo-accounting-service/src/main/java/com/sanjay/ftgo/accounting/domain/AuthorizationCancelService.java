package com.sanjay.ftgo.accounting.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AuthorizationCancelService {

    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;

    public AuthorizationCancelService(AuthorizationRepository authorizationRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       AuthorizationDomainEventPublisher domainEventPublisher) {
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @Transactional
    public void reverse(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }
        List<AuthorizationDomainEvent> events = authorization.reverse();
        authorizationRepository.save(authorization);
        domainEventPublisher.publish(events);
    }
}
