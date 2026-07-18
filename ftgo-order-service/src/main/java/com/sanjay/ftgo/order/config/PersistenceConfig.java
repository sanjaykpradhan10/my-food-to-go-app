package com.sanjay.ftgo.order.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Explicitly scans both this service's own JPA entities/repositories and the shared outbox
 * ones from ftgo-common, since the latter live outside FtgoOrderServiceApplication's default
 * @SpringBootApplication base package. (ftgo-common's OutboxPublisher/KafkaProducerConfig
 * beans don't need scanning here — they're registered automatically via ftgo-common's own
 * Spring Boot auto-configuration, see OutboxAutoConfiguration in ftgo-common.)
 *
 * Kept in its own @Configuration class (rather than directly on
 * FtgoOrderServiceApplication) because @WebMvcTest slice tests only filter
 * out @Configuration-annotated classes during component scanning -
 * annotations placed directly on the @SpringBootApplication class itself are
 * NOT filtered and would otherwise pull in JPA repository beans (which need
 * an entityManagerFactory the web slice doesn't provide) into controller
 * tests like OrderControllerTest.
 */
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.order.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
