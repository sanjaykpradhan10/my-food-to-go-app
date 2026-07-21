package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AccountingEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final OrderCancelSagaService orderCancelSagaService = mock(OrderCancelSagaService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AccountingEventListener listener =
            new AccountingEventListener(orderSagaService, orderCancelSagaService, objectMapper);

    @Test
    void routesCardAuthorizationFailedToReject() {
        String payload = """
                {"eventId":"e1","eventType":"CardAuthorizationFailed","orderId":42,"reason":"declined"}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).reject(42L, "e1");
    }

    @Test
    void routesAuthorizationReversedToOrderCancelSagaService() {
        String payload = """
                {"eventId":"e2","eventType":"AuthorizationReversed","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verify(orderCancelSagaService).confirmCancel(42L, "e2");
        verify(orderSagaService, never()).reject(42L, "e2");
    }
}
