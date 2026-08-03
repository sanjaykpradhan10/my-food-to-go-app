package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.sanjay.ftgo.order.security.SecurityConfig;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// SecurityConfig is imported explicitly: @WebMvcTest doesn't scan @Configuration classes, and
// without it the AuthenticationPrincipalArgumentResolver is never registered, so
// @AuthenticationPrincipal Jwt resolves to null regardless of what jwt() sets in the SecurityContext.
// The real filter chain must stay enabled (no addFilters = false): jwt()'s authentication is only
// propagated into SecurityContextHolder by SecurityContextHolderFilter, which addFilters = false
// would skip - every request in this class supplies a jwt() principal accordingly.
@WebMvcTest(OrderController.class)
@Import(SecurityConfig.class)
class OrderControllerTest {

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

    @Test
    void getsOrderById() throws Exception {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(1L)).thenReturn(java.util.Optional.of(order));

        mockMvc.perform(get("/orders/1").with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.consumerId").value(1))
                .andExpect(jsonPath("$.restaurantId").value(1))
                .andExpect(jsonPath("$.status").value("APPROVAL_PENDING"));
    }

    @Test
    void returns404WhenGettingUnknownOrder() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(java.util.Optional.empty());

        mockMvc.perform(get("/orders/99").with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER"))))
                .andExpect(status().isNotFound());
    }

    @Test
    void createsOrderSuccessfully() throws Exception {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenReturn(order);

        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":2}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.consumerId").value(1))
                .andExpect(jsonPath("$.restaurantId").value(1))
                .andExpect(jsonPath("$.status").value("APPROVAL_PENDING"));
    }

    @Test
    void derivesConsumerIdFromJwtRatherThanRequestBody() throws Exception {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenReturn(order);

        // Body claims consumerId 999, but the authenticated JWT subject is "1" - the controller
        // must trust the JWT, not the body, so orderService is invoked with consumerId 1.
        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":999,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":2}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.consumerId").value(1));

        verify(orderService).createOrder(eq(1L), eq(1L), any());
    }

    @Test
    void returns404WhenRestaurantNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), eq(99L), any())).thenThrow(new RestaurantNotFoundException(99L));

        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":99,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns404WhenMenuItemNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenThrow(new MenuItemNotFoundException(999L, 1L));

        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":999,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns503WhenRestaurantServiceUnavailable() throws Exception {
        when(orderService.createOrder(eq(1L), eq(1L), any()))
                .thenThrow(new RestaurantServiceUnavailableException(1L, new RuntimeException("timeout")));

        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void returns400WhenConsumerIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenRestaurantIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenLineItemsEmpty() throws Exception {
        mockMvc.perform(post("/orders")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
        when(orderTransitions.cancel(eq(5L), any()))
                .thenReturn(new TransitionResult(order, List.of(new com.sanjay.ftgo.order.domain.OrderCancelledEvent(5L))));

        mockMvc.perform(post("/orders/5/cancel")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));

        verify(cancellationSagaTrigger).onOrderCancelled(eq(order), any());
    }

    @Test
    void returns404WhenCancellingUnknownOrder() throws Exception {
        when(orderTransitions.cancel(eq(99L), any())).thenThrow(new OrderNotFoundException(99L));

        mockMvc.perform(post("/orders/99/cancel")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER"))))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenCancellingAnOrderThatCannotBeCancelled() throws Exception {
        when(orderTransitions.cancel(eq(5L), any())).thenThrow(new OrderCannotBeCancelledException(5L));

        mockMvc.perform(post("/orders/5/cancel")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER"))))
                .andExpect(status().isConflict());
    }

    @Test
    void revisesAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING);
        when(orderTransitions.revise(eq(5L), any(), any())).thenReturn(new TransitionResult(order,
                List.of(new com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent(5L, List.of(new OrderLineItem(10L, 5))))));

        mockMvc.perform(post("/orders/5/revise")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REVISION_PENDING"));

        verify(revisionSagaTrigger).onOrderRevised(eq(order), any());
    }

    @Test
    void returns404WhenRevisingUnknownOrder() throws Exception {
        when(orderTransitions.revise(eq(99L), any(), any())).thenThrow(new OrderNotFoundException(99L));

        mockMvc.perform(post("/orders/99/revise")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenRevisingAnOrderNotYetApproved() throws Exception {
        when(orderTransitions.revise(eq(5L), any(), any()))
                .thenThrow(new UnsupportedStateTransitionException(OrderStatus.APPROVAL_PENDING));

        mockMvc.perform(post("/orders/5/revise")
                        .with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER"))).authorities(new SimpleGrantedAuthority("ROLE_CONSUMER")))
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isConflict());
    }
}
