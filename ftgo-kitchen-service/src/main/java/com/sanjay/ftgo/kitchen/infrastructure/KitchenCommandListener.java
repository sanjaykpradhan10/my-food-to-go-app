package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.KitchenCommand;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class KitchenCommandListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenCommandListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public KitchenCommandListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.commands", groupId = "kitchen-service")
    public void onMessage(String payload) {
        KitchenCommand command;
        try {
            command = objectMapper.readValue(payload, KitchenCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen command: {}", payload, e);
            return;
        }
        switch (command.commandType()) {
            case "CreateTicket" ->
                    ticketService.handleCreateTicketCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ConfirmTicket" -> ticketService.handleConfirmTicketCommand(command.eventId(), command.orderId());
            case "CancelTicket" -> ticketService.handleCancelTicketCommand(command.eventId(), command.orderId());
            default -> log.warn("Unknown kitchen command type: {}", command.commandType());
        }
    }
}
