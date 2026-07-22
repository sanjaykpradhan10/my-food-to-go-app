package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderReviseSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KitchenEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final OrderCancelSagaService orderCancelSagaService = mock(OrderCancelSagaService.class);
    private final OrderReviseSagaService orderReviseSagaService = mock(OrderReviseSagaService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KitchenEventListener listener =
            new KitchenEventListener(orderSagaService, orderCancelSagaService, orderReviseSagaService, objectMapper);

    @Test
    void routesTicketConfirmedToApprove() {
        String payload = """
                {"eventId":"e1","eventType":"TicketConfirmed","orderId":42,"ticketId":1,"totalQuantity":null,"reason":null}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).approve(42L, "e1");
    }

    @Test
    void routesTicketCancellationRejectedToOrderCancelSagaService() {
        String payload = """
                {"eventId":"e2","eventType":"TicketCancellationRejected","orderId":42,"ticketId":1,"totalQuantity":null,"reason":"cannot cancel once ready for pickup"}
                """;

        listener.onMessage(payload);

        verify(orderCancelSagaService).rejectCancel(42L, "e2");
        verify(orderSagaService, never()).approve(any(), any());
        verify(orderSagaService, never()).reject(any(), any());
    }

    @Test
    void handlesTicketRevisionRejectedEvent() {
        String payload = """
                {"eventId":"e10","eventType":"TicketRevisionRejected","orderId":42,"reason":"order exceeds kitchen capacity"}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).rejectRevision(42L, "e10");
    }

    @Test
    void handlesTicketRevisionUndoneEvent() {
        String payload = """
                {"eventId":"e11","eventType":"TicketRevisionUndone","orderId":42,"totalQuantity":2}
                """;

        listener.onMessage(payload);

        verify(orderReviseSagaService).finalizeRejectedRevision(42L, "e11");
    }
}
