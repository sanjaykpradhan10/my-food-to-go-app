package com.sanjay.ftgo.gateway.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * spring-boot-starter-oauth2-resource-server auto-configures a default WebFlux security chain
 * that requires an authenticated (valid Bearer JWT) request for every path, including
 * /actuator/health — this would 401 the compose healthcheck and Docker's own container-health
 * probe, and contradicts this project's spec that health endpoints stay unauthenticated.
 * Auth for routed traffic is already enforced independently by JwtValidationFilter (a Gateway
 * GlobalFilter, not this Security chain), so permitting /actuator/** here doesn't weaken it.
 */
@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/actuator/**").permitAll()
                        .anyExchange().permitAll())
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .build();
    }
}
