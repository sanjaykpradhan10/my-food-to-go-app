package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.sanjay.ftgo.order.security.SecurityConfig;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Verifies the instance-based ACL on GET /orders/{id}: ADMIN bypasses the ownership check
// unconditionally, CONSUMER only for their own order.
// SecurityConfig must be imported explicitly: @WebMvcTest doesn't scan @Configuration classes, and
// without it the AuthenticationPrincipalArgumentResolver is never registered, so @AuthenticationPrincipal
// Jwt resolves to null regardless of what jwt() sets in the SecurityContext. The real filter chain
// must stay enabled (no addFilters = false): jwt()'s authentication is only propagated into
// SecurityContextHolder by SecurityContextHolderFilter, which addFilters = false would skip.
@WebMvcTest(OrderController.class)
@Import(SecurityConfig.class)
class OrderControllerAclTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @MockitoBean
    private OrderTransitions orderTransitions;

    @MockitoBean
    private OrderCancellationSagaTrigger cancellationSagaTrigger;

    @MockitoBean
    private OrderRevisionSagaTrigger revisionSagaTrigger;

    @MockitoBean
    private OrderRepository orderRepository;

    private static Order orderOwnedBy(long consumerId) {
        return new Order(1L, consumerId, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
    }

    @Test
    void consumerCanViewTheirOwnOrder() throws Exception {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(orderOwnedBy(42L)));

        mockMvc.perform(get("/orders/1").with(jwt().jwt(b -> b.claim("sub", "42").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isOk());
    }

    @Test
    void consumerIsForbiddenFromViewingAnotherConsumersOrder() throws Exception {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(orderOwnedBy(42L)));

        mockMvc.perform(get("/orders/1").with(jwt().jwt(b -> b.claim("sub", "99").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminCanViewAnyOrderRegardlessOfConsumerId() throws Exception {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(orderOwnedBy(42L)));

        mockMvc.perform(get("/orders/1").with(jwt().jwt(b -> b.claim("sub", "999").claim("roles", List.of("ADMIN")))))
                .andExpect(status().isOk());
    }
}
