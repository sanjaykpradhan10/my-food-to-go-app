package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KitchenEventListenerTest {

    private final SagaJoinService sagaJoinService = mock(SagaJoinService.class);
    private final AuthorizationCancelService authorizationCancelService = mock(AuthorizationCancelService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KitchenEventListener listener =
            new KitchenEventListener(sagaJoinService, authorizationCancelService, objectMapper);

    @Test
    void routesTicketCreatedToSagaJoinService() {
        String payload = """
                {"eventId":"e1","eventType":"TicketCreated","orderId":42,"ticketId":1,"totalQuantity":5,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService).handleKitchenEvent("e1", 42L, "TicketCreated", 5);
        verify(authorizationCancelService, never()).reverse(any(), any(), any());
    }

    @Test
    void routesTicketCancelledToAuthorizationCancelService() {
        String payload = """
                {"eventId":"e2","eventType":"TicketCancelled","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(authorizationCancelService).reverse("e2", 42L, "CancelOrder");
        verify(sagaJoinService, never()).handleKitchenEvent(any(), any(), any(), any());
    }

    @Test
    void ignoresIrrelevantEventTypes() {
        String payload = """
                {"eventId":"e3","eventType":"TicketAccepted","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(sagaJoinService, never()).handleKitchenEvent(any(), any(), any(), any());
        verify(authorizationCancelService, never()).reverse(any(), any(), any());
    }
}
