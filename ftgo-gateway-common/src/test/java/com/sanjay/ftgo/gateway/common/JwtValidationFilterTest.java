package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JwtValidationFilterTest {

    private final ReactiveJwtDecoder jwtDecoder = mock(ReactiveJwtDecoder.class);
    private final JwtValidationFilter filter = new JwtValidationFilter(jwtDecoder);

    @Test
    void rejectsMissingAuthorizationHeaderWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verify(chain, times(0)).filter(exchange);
    }

    @Test
    void rejectsInvalidJwtWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("Authorization", "Bearer bad-token").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(jwtDecoder.decode("bad-token")).thenReturn(Mono.error(new BadJwtException("invalid")));
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void passesThroughWithValidJwt() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("Authorization", "Bearer good-token").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        Jwt jwt = Jwt.withTokenValue("good-token")
                .header("alg", "RS256")
                .claim("sub", "1")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .build();
        when(jwtDecoder.decode("good-token")).thenReturn(Mono.just(jwt));
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        filter.filter(exchange, chain).block();

        verify(chain, times(1)).filter(exchange);
    }
}
