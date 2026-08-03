package com.sanjay.ftgo.gateway.common;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Replaces the Ch.8 X-Api-Key stub (ApiKeyAuthFilter, retired): validates the incoming JWT's
 * signature against the authorization server's JWK Set and rejects with 401 if missing/invalid.
 * Deliberately does NOT re-encode or strip/replace the Authorization header — the same bearer
 * token the client sent is forwarded to backend services unchanged, so each Resource Server
 * validates it again independently (no shared trust boundary between gateway and services).
 */
@Component
public class JwtValidationFilter implements GlobalFilter, Ordered {

    // Exposed so downstream filters (e.g. PerKeyRateLimiterGatewayFilterFactory) can key off the
    // caller's identity without re-decoding/re-validating a token this filter already verified.
    public static final String VALIDATED_JWT_ATTRIBUTE = "com.sanjay.ftgo.gateway.common.VALIDATED_JWT";

    private final ReactiveJwtDecoder jwtDecoder;

    public JwtValidationFilter(ReactiveJwtDecoder jwtDecoder) {
        this.jwtDecoder = jwtDecoder;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String authorization = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return unauthorized(exchange);
        }
        String token = authorization.substring("Bearer ".length());
        return jwtDecoder.decode(token)
                .doOnNext(jwt -> exchange.getAttributes().put(VALIDATED_JWT_ATTRIBUTE, jwt))
                .then(chain.filter(exchange))
                .onErrorResume(JwtException.class, ex -> unauthorized(exchange));
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE + 1;
    }
}
