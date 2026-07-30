package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

// The @Component-annotated filters below live in this library's own package, which a consuming
// gateway module's @SpringBootApplication (e.g. com.sanjay.ftgo.publicgateway) does not
// component-scan. @Import here makes them register via this auto-configuration instead of
// relying on classpath component scanning, which is what actually gets auto-registered via
// META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
@AutoConfiguration
@EnableConfigurationProperties(GatewayApiKeyProperties.class)
@Import({RequestLoggingFilter.class, ApiKeyAuthFilter.class, PerKeyRateLimiterGatewayFilterFactory.class})
public class GatewayCommonAutoConfiguration {
}
