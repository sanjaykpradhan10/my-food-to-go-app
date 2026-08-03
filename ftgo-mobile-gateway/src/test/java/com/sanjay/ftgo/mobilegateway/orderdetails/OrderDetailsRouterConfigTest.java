package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

/**
 * Verifies that the /mobile/orders/{orderId} RouterFunction (which bypasses Spring Cloud
 * Gateway's GlobalFilter chain entirely, since it's not a declared Gateway route) replicates
 * JwtValidationFilter's auth check inline instead of leaving the endpoint unauthenticated.
 */
class OrderDetailsRouterConfigTest {

    private static final String VALID_TOKEN = "valid-jwt";

    private WebTestClient client(OrderDetailsHandler handler, ReactiveJwtDecoder jwtDecoder) {
        RouterFunction<ServerResponse> route =
                new OrderDetailsRouterConfig().orderDetailsRoute(handler, jwtDecoder);
        return WebTestClient.bindToRouterFunction(route).build();
    }

    private ReactiveJwtDecoder decoderAcceptingOnly(String validToken) {
        ReactiveJwtDecoder jwtDecoder = Mockito.mock(ReactiveJwtDecoder.class);
        Jwt jwt = Jwt.withTokenValue(validToken)
                .header("alg", "none")
                .claim("sub", "consumer1")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .build();
        Mockito.when(jwtDecoder.decode(validToken)).thenReturn(Mono.just(jwt));
        Mockito.when(jwtDecoder.decode(Mockito.argThat(token -> !validToken.equals(token))))
                .thenReturn(Mono.error(new JwtException("invalid token")));
        return jwtDecoder;
    }

    @Test
    void missingAuthorizationHeaderIsRejectedWith401() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);

        client(handler, decoderAcceptingOnly(VALID_TOKEN)).get().uri("/mobile/orders/1")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.UNAUTHORIZED);

        Mockito.verifyNoInteractions(handler);
    }

    @Test
    void invalidTokenIsRejectedWith401() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);

        client(handler, decoderAcceptingOnly(VALID_TOKEN)).get().uri("/mobile/orders/1")
                .header("Authorization", "Bearer not-a-real-jwt")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.UNAUTHORIZED);

        Mockito.verifyNoInteractions(handler);
    }

    @Test
    void validTokenPassesThroughToHandler() {
        OrderDetailsHandler handler = Mockito.mock(OrderDetailsHandler.class);
        OrderDetails stubDetails = new OrderDetails(
                new SectionResult.Found<>("{\"id\":1}"),
                new SectionResult.Found<>("{\"ticketId\":1}"),
                new SectionResult.Found<>("{\"status\":\"AUTHORIZED\"}"),
                new SectionResult.Found<>("{\"status\":\"SCHEDULED\"}"));
        Mockito.when(handler.fetchOrderDetails(1L)).thenReturn(Mono.just(stubDetails));

        client(handler, decoderAcceptingOnly(VALID_TOKEN)).get().uri("/mobile/orders/1")
                .header("Authorization", "Bearer " + VALID_TOKEN)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON);

        Mockito.verify(handler).fetchOrderDetails(1L);
    }
}
