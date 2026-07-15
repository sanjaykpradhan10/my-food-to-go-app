package com.sanjay.ftgo.kitchen.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TicketService {

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;

    public TicketService(TicketRepository ticketRepository, ProcessedEventRepository processedEventRepository) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
        ticketRepository.save(new Ticket(event.orderId(), "CREATED"));
    }
}
