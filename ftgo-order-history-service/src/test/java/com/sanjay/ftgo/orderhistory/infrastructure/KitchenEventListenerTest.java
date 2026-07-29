package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class KitchenEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final KitchenEventListener listener = new KitchenEventListener(orderViewService, new ObjectMapper());

    @Test
    void onTicketAcceptedCallsHandleKitchenEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"TicketAccepted","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleKitchenEvent("evt-1", "TicketAccepted", 42L);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
