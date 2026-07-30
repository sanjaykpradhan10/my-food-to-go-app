package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PerKeyRateLimiterGatewayFilterFactoryTest {

    @Test
    void allowsRequestsWithinLimitAndRejectsBeyondIt() {
        PerKeyRateLimiterGatewayFilterFactory factory = new PerKeyRateLimiterGatewayFilterFactory();
        PerKeyRateLimiterGatewayFilterFactory.Config config = new PerKeyRateLimiterGatewayFilterFactory.Config();
        config.setRequestsPerSecond(2);
        GatewayFilter filter = factory.apply(config);

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());

        ServerWebExchange first = exchangeWithKey("key-a");
        ServerWebExchange second = exchangeWithKey("key-a");
        ServerWebExchange third = exchangeWithKey("key-a");

        filter.filter(first, chain).block();
        filter.filter(second, chain).block();
        filter.filter(third, chain).block();

        assertThat(first.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(second.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(third.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    private ServerWebExchange exchangeWithKey(String key) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").header("X-Api-Key", key).build());
    }
}
