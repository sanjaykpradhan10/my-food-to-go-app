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
import static org.mockito.Mockito.when;

class RequestLoggingFilterTest {

    @Test
    void passesRequestThroughAndCompletesNormally() {
        RequestLoggingFilter filter = new RequestLoggingFilter();
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        exchange.getResponse().setStatusCode(HttpStatus.OK);

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(exchange)).thenReturn(Mono.empty());

        Mono<Void> result = filter.filter(exchange, chain);

        assertThat(result.blockOptional()).isEmpty();
    }

    @Test
    void hasHighestPrecedenceOrdering() {
        RequestLoggingFilter filter = new RequestLoggingFilter();
        assertThat(filter.getOrder()).isEqualTo(Integer.MIN_VALUE);
    }
}
