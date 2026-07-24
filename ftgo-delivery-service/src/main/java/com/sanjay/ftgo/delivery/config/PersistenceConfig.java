package com.sanjay.ftgo.delivery.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoDeliveryServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering —
// see ftgo-kitchen-service's PersistenceConfig for the concrete failure this pattern avoids.
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.delivery.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.delivery.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
