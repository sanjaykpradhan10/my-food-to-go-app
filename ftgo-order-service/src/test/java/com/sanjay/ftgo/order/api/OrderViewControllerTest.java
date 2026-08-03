package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import com.sanjay.ftgo.order.infrastructure.VirtualThreadExecutorConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Uses @Import(VirtualThreadExecutorConfig.class) rather than mocking-then-overriding the
// orderViewExecutor bean: a @MockitoBean ExecutorService is a no-op mock (execute() does
// nothing), so any submitted CompletableFuture never completes and join() hangs forever.
// Importing the real virtual-thread executor sidesteps that deadlock entirely.
// addFilters = false: this slice test predates Ch.11 security and exercises OrderViewController's
// business logic, not auth - it never sends a bearer token, so the Spring Security filter chain
// would 401 every request. Auth enforcement is verified at the e2e layer per the design spec.
@WebMvcTest(OrderViewController.class)
@Import(VirtualThreadExecutorConfig.class)
@AutoConfigureMockMvc(addFilters = false)
class OrderViewControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderRepository orderRepository;

    @MockitoBean
    private RestaurantServicePort restaurantServicePort;

    @MockitoBean
    private KitchenServicePort kitchenServicePort;

    @MockitoBean
    private AccountingServicePort accountingServicePort;

    @MockitoBean
    private DeliveryServicePort deliveryServicePort;

    @Test
    void returnsAllFoundSections() throws Exception {
        Order order = new Order(1L, 42L, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(restaurantServicePort.findRestaurantForView(7L))
                .thenReturn(new Found<>(new RestaurantInfo(7L, "Ajanta", List.of())));
        when(kitchenServicePort.findTicket(1L))
                .thenReturn(new Found<>(new TicketInfo(1L, 1L, "ACCEPTED", null)));
        when(accountingServicePort.findAuthorization(1L))
                .thenReturn(new Found<>(new AuthorizationInfo(1L, 1L, "AUTHORIZED")));
        when(deliveryServicePort.findDelivery(1L))
                .thenReturn(new Found<>(new DeliveryInfo(1L, 1L, "SCHEDULED", 3L)));

        mockMvc.perform(get("/orders/1/view"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.order.id").value(1))
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.authorization.data.status").value("AUTHORIZED"))
                .andExpect(jsonPath("$.delivery.data.courierId").value(3));
    }

    @Test
    void returns404WhenOrderNotFound() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/orders/99/view")).andExpect(status().isNotFound());
    }

    @Test
    void degradesIndividualSectionsIndependently() throws Exception {
        Order order = new Order(1L, 42L, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(restaurantServicePort.findRestaurantForView(7L))
                .thenReturn(new Found<>(new RestaurantInfo(7L, "Ajanta", List.of())));
        when(kitchenServicePort.findTicket(1L)).thenReturn(new NotFound<>());
        when(accountingServicePort.findAuthorization(1L)).thenReturn(new NotFound<>());
        when(deliveryServicePort.findDelivery(1L)).thenReturn(new Unavailable<>("timeout"));

        mockMvc.perform(get("/orders/1/view"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data").doesNotExist())
                .andExpect(jsonPath("$.authorization.data").doesNotExist())
                .andExpect(jsonPath("$.delivery.reason").value("timeout"));
    }
}
