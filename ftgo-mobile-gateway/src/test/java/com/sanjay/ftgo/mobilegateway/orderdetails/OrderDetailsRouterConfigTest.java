package com.sanjay.ftgo.mobilegateway.orderdetails;

import com.sanjay.ftgo.gateway.common.GatewayApiKeyProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

/**
 * Verifies that the /mobile/orders/{orderId} RouterFunction (which bypasses Spring Cloud
 * Gateway's GlobalFilter chain entirely, since it's not a declared Gateway route) replicates
 * ApiKeyAuthFilter's auth check inline instead of leaving the endpoint unauthenticated.
 */
class OrderDetailsRouterConfigTest {

    private static final String VALID_KEY = "secret-123";

    private WebTestClient client(OrderDetailsHandler handler) {
        GatewayApiKeyProperties properties = new GatewayApiKeyProperties(VALID_KEY);
        RouterFunction<ServerResponse> route =
                new OrderDetailsRouterConfig().orderDetailsRoute(handler, properties);
        return WebTestClient.bindToRouterFunction(route).build();
    }

    @Test
    void missingApiKeyIsRejectedWith401() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);

        client(handler).get().uri("/mobile/orders/1")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.UNAUTHORIZED);

        Mockito.verifyNoInteractions(handler);
    }

    @Test
    void wrongApiKeyIsRejectedWith401() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);

        client(handler).get().uri("/mobile/orders/1")
                .header("X-Api-Key", "wrong-key")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.UNAUTHORIZED);

        Mockito.verifyNoInteractions(handler);
    }

    @Test
    void correctApiKeyPassesThroughToHandler() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);
        OrderDetails stubDetails = new OrderDetails(
                new SectionResult.Found<>("{\"id\":1}"),
                new SectionResult.Found<>("{\"ticketId\":1}"),
                new SectionResult.Found<>("{\"status\":\"AUTHORIZED\"}"),
                new SectionResult.Found<>("{\"status\":\"SCHEDULED\"}"));
        Mockito.when(handler.fetchOrderDetails(1L)).thenReturn(Mono.just(stubDetails));

        client(handler).get().uri("/mobile/orders/1")
                .header("X-Api-Key", VALID_KEY)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON);

        Mockito.verify(handler).fetchOrderDetails(1L);
    }
}
