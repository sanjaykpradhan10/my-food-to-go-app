package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AccountingCommandListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AccountingCommandListener listener =
            new AccountingCommandListener(sagaJoinService, authorizationCancelService, objectMapper);

    @Test
    void routesAuthorizeCardToSagaJoinService() {
        String payload = """
                {"eventId":"e1","commandType":"AuthorizeCard","orderId":42,"totalQuantity":5,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleAuthorizeCardCommand("e1", 42L, 5);
        verify(authorizationCancelService, never()).reverseForCommand(any(), any(), any());
    }

    @Test
    void routesReverseAuthorizationToAuthorizationCancelService() {
        String payload = """
                {"eventId":"e2","commandType":"ReverseAuthorization","orderId":42,"totalQuantity":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverseForCommand("e2", 42L, "CancelOrder");
        verify(sagaJoinService, never()).handleAuthorizeCardCommand(any(), any(), any());
    }
}
