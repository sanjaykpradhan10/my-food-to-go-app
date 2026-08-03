package com.sanjay.ftgo.mobilegateway.orderdetails;

import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;

@Configuration
public class OrderDetailsRouterConfig {

    // This endpoint is a WebFlux RouterFunction, not a declared Spring Cloud Gateway route, so
    // Gateway's GlobalFilters (JwtValidationFilter, RequestLoggingFilter) never run for it -
    // those only fire for requests matched by a RouteLocator. JWT validation is replicated here
    // inline (same NimbusReactiveJwtDecoder auto-configured by gateway-common) so the endpoint
    // isn't left wide open; request logging / rate-limiting for this endpoint are a known
    // follow-up gap, tracked separately, not fixed here.

    // @LoadBalanced lets "http://ftgo-order-service"-style authority-only URIs resolve via
    // Eureka, matching order-service's existing composition pattern from Ch.7 (see its own
    // composed order-view endpoint) rather than hardcoding host:port for each backend.
    @LoadBalanced
    @Bean
    public WebClient.Builder loadBalancedWebClientBuilder() {
        return WebClient.builder();
    }

    @Bean
    public BackendClients backendClients(WebClient.Builder builder) {
        return new BackendClients(
                builder.baseUrl("http://ftgo-order-service").build(),
                builder.baseUrl("http://ftgo-kitchen-service").build(),
                builder.baseUrl("http://ftgo-accounting-service").build(),
                builder.baseUrl("http://ftgo-delivery-service").build());
    }

    @Bean
    public OrderDetailsHandler orderDetailsHandler(BackendClients clients, ReactiveCircuitBreakerFactory circuitBreakerFactory) {
        return new OrderDetailsHandler(clients, circuitBreakerFactory);
    }

    @Bean
    public RouterFunction<ServerResponse> orderDetailsRoute(OrderDetailsHandler handler, ReactiveJwtDecoder jwtDecoder) {
        RouterFunction<ServerResponse> route = RouterFunctions.route(GET("/mobile/orders/{orderId}"), request -> {
            Long orderId;
            try {
                orderId = Long.valueOf(request.pathVariable("orderId"));
            } catch (NumberFormatException e) {
                return ServerResponse.status(HttpStatus.BAD_REQUEST).bodyValue("orderId must be numeric");
            }
            String authorization = request.headers().firstHeader(HttpHeaders.AUTHORIZATION);
            String token = authorization.substring("Bearer ".length());
            return handler.fetchOrderDetails(orderId, token)
                    .flatMap(details -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(details));
        });

        return route.filter((request, next) -> {
            String authorization = request.headers().firstHeader(HttpHeaders.AUTHORIZATION);
            if (authorization == null || !authorization.startsWith("Bearer ")) {
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).build();
            }
            String token = authorization.substring("Bearer ".length());
            return jwtDecoder.decode(token)
                    .then(Mono.defer(() -> next.handle(request)))
                    .onErrorResume(JwtException.class, ex -> ServerResponse.status(HttpStatus.UNAUTHORIZED).build());
        });
    }
}
