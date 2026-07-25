package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final OrderEventListener listener = new OrderEventListener(orderViewService, new ObjectMapper());

    @Test
    void onOrderCreatedCallsHandleOrderEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"OrderCreated","orderId":42,"consumerId":1,"restaurantId":7,
                 "lineItems":[{"menuItemId":10,"quantity":2}]}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of(new OrderViewLineItem(10L, 2)));
    }

    @Test
    void onOrderApprovedCallsHandleOrderEventWithNullOptionalFields() {
        String payload = """
                {"eventId":"evt-2","eventType":"OrderApproved","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
