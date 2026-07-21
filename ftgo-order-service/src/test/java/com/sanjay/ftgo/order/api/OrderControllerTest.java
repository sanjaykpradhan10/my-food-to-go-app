package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @MockitoBean
    private OrderRepository orderRepository;

    @MockitoBean
    private OrderDomainEventPublisher domainEventPublisher;

    @MockitoBean
    private OrderCancellationSagaTrigger cancellationSagaTrigger;

    @Test
    void createsOrderSuccessfully() throws Exception {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderService.createOrder(eq(1L), eq(1L), any())).thenReturn(order);

        mockMvc.perform(post("/orders")
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
    void returns404WhenRestaurantNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), eq(99L), any())).thenThrow(new RestaurantNotFoundException(99L));

        mockMvc.perform(post("/orders")
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
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void returns400WhenConsumerIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenRestaurantIdMissing() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenLineItemsEmpty() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"consumerId":1,"restaurantId":1,"lineItems":[]}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void cancelsAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCEL_PENDING"));

        verify(cancellationSagaTrigger).onOrderCancelled(eq(order), any());
    }

    @Test
    void returns404WhenCancellingUnknownOrder() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/orders/99/cancel"))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenCancellingAnOrderThatCannotBeCancelled() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/cancel"))
                .andExpect(status().isConflict());
    }

    @Test
    void revisesAnApprovedOrder() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REVISION_PENDING"));
    }

    @Test
    void returns404WhenRevisingUnknownOrder() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(post("/orders/99/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns409WhenRevisingAnOrderNotYetApproved() throws Exception {
        Order order = new Order(5L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        mockMvc.perform(post("/orders/5/revise")
                        .contentType("application/json")
                        .content("""
                                {"lineItems":[{"menuItemId":10,"quantity":5}]}
                                """))
                .andExpect(status().isConflict());
    }
}
