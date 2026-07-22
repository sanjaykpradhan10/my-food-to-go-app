package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AccountingCommand;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.AuthorizationReviseService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class AccountingCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingCommandListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final AuthorizationReviseService authorizationReviseService;
    private final ObjectMapper objectMapper;

    public AccountingCommandListener(SagaJoinService sagaJoinService,
                                      AuthorizationCancelService authorizationCancelService,
                                      AuthorizationReviseService authorizationReviseService,
                                      ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
        this.authorizationReviseService = authorizationReviseService;
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
        switch (command.commandType()) {
            case "AuthorizeCard" ->
                    sagaJoinService.handleAuthorizeCardCommand(command.eventId(), command.orderId(), command.totalQuantity());
            case "ReverseAuthorization" ->
                    authorizationCancelService.reverseForCommand(command.eventId(), command.orderId(), command.sagaType());
            case "ReviseAuthorization" ->
                    authorizationReviseService.reviseForCommand(command.eventId(), command.orderId(), command.totalQuantity(), command.sagaType());
            default -> log.warn("Unknown accounting command type: {}", command.commandType());
        }
    }
}
