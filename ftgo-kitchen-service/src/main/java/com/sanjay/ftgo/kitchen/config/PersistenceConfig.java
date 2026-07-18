package com.sanjay.ftgo.kitchen.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoKitchenServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering,
// which only excludes @Component/@Configuration beans discovered via scan — see order-service's
// PersistenceConfig (Task 5) for the concrete failure this pattern avoids.
// (ftgo-common's OutboxPublisher/KafkaProducerConfig beans don't need scanning here — they're
// registered automatically via ftgo-common's own Spring Boot auto-configuration, see
// OutboxAutoConfiguration in ftgo-common.)
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
