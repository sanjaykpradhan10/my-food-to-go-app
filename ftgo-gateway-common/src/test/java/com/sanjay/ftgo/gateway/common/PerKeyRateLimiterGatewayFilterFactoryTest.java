package com.sanjay.ftgo.gateway.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;

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

        ServerWebExchange first = exchangeWithSubject("consumer-a");
        ServerWebExchange second = exchangeWithSubject("consumer-a");
        ServerWebExchange third = exchangeWithSubject("consumer-a");

        filter.filter(first, chain).block();
        filter.filter(second, chain).block();
        filter.filter(third, chain).block();

        assertThat(first.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(second.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(third.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void differentJwtSubjectsGetIndependentRateLimitWindows() {
        PerKeyRateLimiterGatewayFilterFactory factory = new PerKeyRateLimiterGatewayFilterFactory();
        PerKeyRateLimiterGatewayFilterFactory.Config config = new PerKeyRateLimiterGatewayFilterFactory.Config();
        config.setRequestsPerSecond(1);
        GatewayFilter filter = factory.apply(config);

        GatewayFilterChain chain = mock(GatewayFilterChain.class);
        when(chain.filter(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());

        // Each subject's first request should succeed under its own bucket, even though a
        // second request from consumer-a alone would have exceeded the requestsPerSecond=1 cap.
        ServerWebExchange consumerAFirst = exchangeWithSubject("consumer-a");
        ServerWebExchange consumerASecond = exchangeWithSubject("consumer-a");
        ServerWebExchange consumerBFirst = exchangeWithSubject("consumer-b");

        filter.filter(consumerAFirst, chain).block();
        filter.filter(consumerASecond, chain).block();
        filter.filter(consumerBFirst, chain).block();

        assertThat(consumerAFirst.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(consumerASecond.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(consumerBFirst.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    private ServerWebExchange exchangeWithSubject(String subject) {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/mobile/orders/1").build());
        Jwt jwt = Jwt.withTokenValue("token-" + subject)
                .header("alg", "none")
                .claim("sub", subject)
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .build();
        exchange.getAttributes().put(JwtValidationFilter.VALIDATED_JWT_ATTRIBUTE, jwt);
        return exchange;
    }
}
