package com.sanjay.ftgo.order.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

// @EnableWebSecurity (in addition to @EnableMethodSecurity) is required so that @WebMvcTest slice
// tests importing this class pick up AuthenticationPrincipalArgumentResolver - without it,
// @AuthenticationPrincipal Jwt silently resolves to null in a sliced ApplicationContext even
// though the same wiring works automatically in the full application context.
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, RolesClaimJwtAuthenticationConverter converter)
            throws Exception {
        http.authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                        .anyRequest().authenticated())
                .csrf(csrf -> csrf.disable())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(converter)));
        return http.build();
    }

    @Bean
    public RolesClaimJwtAuthenticationConverter rolesClaimJwtAuthenticationConverter() {
        return new RolesClaimJwtAuthenticationConverter();
    }
}
