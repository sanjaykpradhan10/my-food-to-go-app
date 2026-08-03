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
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import com.sanjay.ftgo.order.security.SecurityConfig;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Uses @Import(VirtualThreadExecutorConfig.class) rather than mocking-then-overriding the
// orderViewExecutor bean: a @MockitoBean ExecutorService is a no-op mock (execute() does
// nothing), so any submitted CompletableFuture never completes and join() hangs forever.
// Importing the real virtual-thread executor sidesteps that deadlock entirely.
// SecurityConfig is imported explicitly: @WebMvcTest doesn't scan @Configuration classes, and
// without it the AuthenticationPrincipalArgumentResolver is never registered, so
// @AuthenticationPrincipal Jwt resolves to null regardless of what jwt() sets in the SecurityContext.
// The real filter chain must stay enabled (no addFilters = false): jwt()'s authentication is only
// propagated into SecurityContextHolder by SecurityContextHolderFilter, which addFilters = false
// would skip - every request in this class supplies a jwt() principal accordingly.
@WebMvcTest(OrderViewController.class)
@Import({VirtualThreadExecutorConfig.class, SecurityConfig.class})
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

        mockMvc.perform(get("/orders/1/view").with(jwt().jwt(b -> b.claim("sub", "42").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.order.id").value(1))
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.authorization.data.status").value("AUTHORIZED"))
                .andExpect(jsonPath("$.delivery.data.courierId").value(3));
    }

    @Test
    void forbidsConsumerFromViewingAnotherConsumersOrder() throws Exception {
        Order order = new Order(1L, 42L, 7L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));

        mockMvc.perform(get("/orders/1/view").with(jwt().jwt(b -> b.claim("sub", "99").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminCanViewAnyOrderRegardlessOfConsumerId() throws Exception {
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

        mockMvc.perform(get("/orders/1/view").with(jwt().jwt(b -> b.claim("sub", "999").claim("roles", List.of("ADMIN")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.order.id").value(1));
    }

    @Test
    void returns404WhenOrderNotFound() throws Exception {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/orders/99/view").with(jwt().jwt(b -> b.claim("sub", "1").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isNotFound());
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

        mockMvc.perform(get("/orders/1/view").with(jwt().jwt(b -> b.claim("sub", "42").claim("roles", List.of("CONSUMER")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.restaurant.data.name").value("Ajanta"))
                .andExpect(jsonPath("$.ticket.data").doesNotExist())
                .andExpect(jsonPath("$.authorization.data").doesNotExist())
                .andExpect(jsonPath("$.delivery.reason").value("timeout"));
    }
}
