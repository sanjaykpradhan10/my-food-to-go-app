package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeliveryCommandListenerTest {

    private final DeliveryService deliveryService = mock(DeliveryService.class);
    private final DeliveryCommandListener listener = new DeliveryCommandListener(deliveryService, new ObjectMapper());

    @Test
    void onScheduleDeliveryCommandCallsHandleScheduleDeliveryCommand() {
        String payload = """
                {"eventId":"evt-1","commandType":"ScheduleDelivery","orderId":42,"restaurantId":7,"sagaType":"CreateOrder"}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleScheduleDeliveryCommand("evt-1", 42L, 7L);
    }

    @Test
    void onReleaseDeliveryCommandCallsHandleReleaseDeliveryCommand() {
        String payload = """
                {"eventId":"evt-2","commandType":"ReleaseDelivery","orderId":42,"restaurantId":null,"sagaType":"CancelOrder"}
                """;

        listener.onMessage(payload);

        verify(deliveryService).handleReleaseDeliveryCommand("evt-2", 42L, "CancelOrder");
    }
}
