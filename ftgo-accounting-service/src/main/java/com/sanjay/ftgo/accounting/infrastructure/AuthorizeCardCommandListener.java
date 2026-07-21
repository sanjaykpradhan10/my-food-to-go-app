package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AccountingCommand;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class AuthorizeCardCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AuthorizeCardCommandListener.class);

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public AuthorizeCardCommandListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.commands", groupId = "accounting-service")
    public void onMessage(String payload) {
        AccountingCommand command;
        try {
            command = objectMapper.readValue(payload, AccountingCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting command: {}", payload, e);
            return;
        }
        sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
    }
}
