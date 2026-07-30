package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApiKeyAuthFilterTest {

    private final GatewayApiKeyProperties properties = new GatewayApiKeyProperties("secret-123");
    private final ApiKeyAuthFilter filter = new ApiKeyAuthFilter(properties);

    @Test
    void rejectsMissingApiKeyWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verify(chain, times(0)).filter(exchange);
    }

    @Test
    void rejectsWrongApiKeyWith401() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", "wrong").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void passesThroughWithCorrectApiKey() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", "secret-123").build());
        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        filter.filter(exchange, chain).block();

        verify(chain, times(1)).filter(exchange);
    }
}
