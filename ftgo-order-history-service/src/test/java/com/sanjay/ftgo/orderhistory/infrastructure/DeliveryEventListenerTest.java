package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DeliveryEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final DeliveryEventListener listener = new DeliveryEventListener(orderViewService, new ObjectMapper());

    @Test
    void onDeliveryScheduledCallsHandleDeliveryEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"DeliveryScheduled","orderId":42,"courierId":3}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleDeliveryEvent("evt-1", "DeliveryScheduled", 42L, 3L);
    }

    @Test
    void deliveryPickedUpCallsHandleDeliveryEvent() {
        String payload = """
                {"eventId":"evt-2","eventType":"DeliveryPickedUp","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleDeliveryEvent("evt-2", "DeliveryPickedUp", 42L, null);
    }

    @Test
    void deliveryDeliveredCallsHandleDeliveryEvent() {
        String payload = """
                {"eventId":"evt-3","eventType":"DeliveryDelivered","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleDeliveryEvent("evt-3", "DeliveryDelivered", 42L, null);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
