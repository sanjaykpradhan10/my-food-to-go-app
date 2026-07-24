package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final OrderSagaService orderSagaService = mock(OrderSagaService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(orderSagaService, new ObjectMapper());

    @Test
    void deliverySchedulingFailedRejectsOrder() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliverySchedulingFailed","orderId":42,"reason":"no courier available"}
                """;

        listener.onMessage(payload);

        verify(orderSagaService).reject(42L, "evt-1");
    }

    @Test
    void ignoresDeliveryScheduled() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"reason":null}
                """;

        listener.onMessage(payload);

        verifyNoInteractions(orderSagaService);
    }
}
