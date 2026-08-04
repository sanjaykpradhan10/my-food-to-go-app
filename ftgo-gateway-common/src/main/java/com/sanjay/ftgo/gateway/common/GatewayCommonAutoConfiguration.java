package com.sanjay.ftgo.gateway.common;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

// The @Component-annotated filters below live in this library's own package, which a consuming
// gateway module's @SpringBootApplication (e.g. com.sanjay.ftgo.publicgateway) does not
// component-scan. @Import here makes them register via this auto-configuration instead of
// relying on classpath component scanning, which is what actually gets auto-registered via
// META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
@AutoConfiguration
@EnableConfigurationProperties(GatewayJwtProperties.class)
@Import({RequestLoggingFilter.class, JwtValidationFilter.class, PerKeyRateLimiterGatewayFilterFactory.class, GatewaySecurityConfig.class})
public class GatewayCommonAutoConfiguration {

    @Bean
    public ReactiveJwtDecoder reactiveJwtDecoder(GatewayJwtProperties properties) {
        return NimbusReactiveJwtDecoder.withJwkSetUri(properties.jwkSetUri()).build();
    }

    // Spring Boot 3.5's ContextPropagationAutoConfiguration is expected to enable this
    // automatically once micrometer's context-propagation and reactor-core are both on the
    // classpath, but verification (ContextPropagationTest in each gateway module) showed
    // Hooks.isAutomaticContextPropagationEnabled() is still false at runtime in this project's
    // configuration, so it is enabled explicitly here to guarantee trace context survives the
    // reactive filter chain (RequestLoggingFilter, JwtValidationFilter) in both gateways.
    @Bean
    public InitializingBean enableReactorContextPropagation() {
        return () -> reactor.core.publisher.Hooks.enableAutomaticContextPropagation();
    }
}
