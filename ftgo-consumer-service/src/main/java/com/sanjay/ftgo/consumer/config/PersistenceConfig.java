package com.sanjay.ftgo.consumer.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoConsumerServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering,
// which only excludes @Component/@Configuration beans discovered via scan — see order-service's
// PersistenceConfig (Task 5) for the concrete failure this pattern avoids.
// @ComponentScan is required in addition to @EntityScan/@EnableJpaRepositories because
// OutboxPublisher (@Component) and KafkaProducerConfig (@Configuration) in ftgo-common
// are beans, not entities/repositories — without it they're silently never registered
// and the outbox poller never runs, with no startup error since nothing else requires them.
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.consumer.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.consumer.domain", "com.sanjay.ftgo.common.outbox"})
@ComponentScan(basePackages = "com.sanjay.ftgo.common.outbox")
public class PersistenceConfig {
}
