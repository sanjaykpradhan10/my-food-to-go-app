package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderEventListenerContractTest {

    // The contract's example body (Task 4, Step 1) as literal JSON, matching what Stub Runner
    // would replay from the published contract for a messaging interaction of this kind. This
    // directly feeds the example into the real listener method rather than requiring a live
    // embedded broker on the consumer side, since OrderEventListener.onMessage() takes the raw
    // payload string directly -- the same boundary Ch.9's KitchenEventListenerTest already
    // exercises for a sibling listener.
    private static final String ORDER_CREATED_CONTRACT_BODY = """
            {"eventId":"11111111-1111-1111-1111-111111111111","eventType":"OrderCreated",
             "orderId":1223232,"consumerId":1,"restaurantId":1,
             "lineItems":[{"menuItemId":10,"quantity":2}]}
            """;

    @Test
    void invokesOrderViewServiceWithFieldsFromTheContract() {
        OrderViewService orderViewService = mock(OrderViewService.class);
        OrderEventListener listener = new OrderEventListener(orderViewService, new ObjectMapper());

        listener.onMessage(ORDER_CREATED_CONTRACT_BODY);

        ArgumentCaptor<List<OrderViewLineItem>> lineItemsCaptor = ArgumentCaptor.forClass(List.class);
        verify(orderViewService).handleOrderEvent(
                org.mockito.ArgumentMatchers.eq("11111111-1111-1111-1111-111111111111"),
                org.mockito.ArgumentMatchers.eq("OrderCreated"),
                org.mockito.ArgumentMatchers.eq(1223232L),
                org.mockito.ArgumentMatchers.eq(1L),
                org.mockito.ArgumentMatchers.eq(1L),
                lineItemsCaptor.capture());
        assertEquals(List.of(new OrderViewLineItem(10L, 2)), lineItemsCaptor.getValue());
    }
}
