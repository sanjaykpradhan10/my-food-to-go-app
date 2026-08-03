package com.sanjay.ftgo.orderhistory.api;

import com.sanjay.ftgo.orderhistory.domain.OrderView;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// addFilters = false: this slice test predates Ch.11 security and exercises OrderHistoryController's
// business logic, not auth - it never sends a bearer token, so the Spring Security filter chain
// would 401 every request. Auth enforcement is verified at the e2e layer per the design spec.
@WebMvcTest(OrderHistoryController.class)
@AutoConfigureMockMvc(addFilters = false)
class OrderHistoryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderViewRepository orderViewRepository;

    @Test
    void returnsOrderViewWhenFound() throws Exception {
        OrderView view = new OrderView(42L);
        view.setConsumerId(1L);
        view.setRestaurantId(7L);
        view.setOrderStatus("APPROVED");
        view.setTicketStatus("ACCEPTED");
        view.setAuthorizationStatus("AUTHORIZED");
        view.setDeliveryStatus("SCHEDULED");
        view.setCourierId(3L);
        view.setLineItems(List.of(new OrderViewLineItem(10L, 2)));
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(view));

        mockMvc.perform(get("/order-views/42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(42))
                .andExpect(jsonPath("$.orderStatus").value("APPROVED"))
                .andExpect(jsonPath("$.ticketStatus").value("ACCEPTED"))
                .andExpect(jsonPath("$.authorizationStatus").value("AUTHORIZED"))
                .andExpect(jsonPath("$.deliveryStatus").value("SCHEDULED"))
                .andExpect(jsonPath("$.courierId").value(3))
                .andExpect(jsonPath("$.lineItems[0].menuItemId").value(10));
    }

    @Test
    void returns404WhenNotFound() throws Exception {
        when(orderViewRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/order-views/99")).andExpect(status().isNotFound());
    }
}
