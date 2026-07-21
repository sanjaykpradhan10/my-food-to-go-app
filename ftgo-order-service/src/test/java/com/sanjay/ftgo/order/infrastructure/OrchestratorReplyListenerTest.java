package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrchestratorReplyListenerTest {

    private final CreateOrderSagaOrchestrator createOrderSagaOrchestrator = mock(CreateOrderSagaOrchestrator.class);
    private final CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator = mock(CancelOrderSagaOrchestrator.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OrchestratorReplyListener listener = new OrchestratorReplyListener(
            createOrderSagaOrchestrator, cancelOrderSagaOrchestrator, objectMapper);

    @Test
    void routesCreateOrderReplyToCreateOrderSagaOrchestrator() {
        String payload = """
                {"eventId":"e1","participant":"kitchen","eventType":"TicketCreated","orderId":42,"reason":null,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(createOrderSagaOrchestrator).handleReply("e1", "kitchen", "TicketCreated", 42L, null);
        verifyNoInteractions(cancelOrderSagaOrchestrator);
    }

    @Test
    void routesCancelOrderReplyToCancelOrderSagaOrchestrator() {
        String payload = """
                {"eventId":"e2","participant":"kitchen","eventType":"TicketCancelled","orderId":42,"reason":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(cancelOrderSagaOrchestrator).handleReply("e2", "kitchen", "TicketCancelled", 42L, null);
        verifyNoInteractions(createOrderSagaOrchestrator);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(createOrderSagaOrchestrator);
        verifyNoInteractions(cancelOrderSagaOrchestrator);
    }
}
