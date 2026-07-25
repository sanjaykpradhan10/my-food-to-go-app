package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AccountingEventListenerTest {

    private final OrderViewService orderViewService = mock(OrderViewService.class);
    private final AccountingEventListener listener = new AccountingEventListener(orderViewService, new ObjectMapper());

    @Test
    void onCardAuthorizedCallsHandleAccountingEvent() {
        String payload = """
                {"eventId":"evt-1","eventType":"CardAuthorized","orderId":42}
                """;

        listener.onMessage(payload);

        verify(orderViewService).handleAccountingEvent("evt-1", "CardAuthorized", 42L);
    }

    @Test
    void skipsMalformedPayload() {
        listener.onMessage("not json");

        verifyNoInteractions(orderViewService);
    }
}
