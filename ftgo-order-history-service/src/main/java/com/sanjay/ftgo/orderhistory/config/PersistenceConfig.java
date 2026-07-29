package com.sanjay.ftgo.orderhistory.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoOrderHistoryServiceApplication because @EntityScan/@EnableJpaRepositories
// placed directly on the @SpringBootApplication class bypass @WebMvcTest's slice filtering,
// which only excludes @Component/@Configuration beans discovered via scan — see order-service's
// PersistenceConfig for the concrete failure this pattern avoids.
// (ftgo-common's KafkaProducerConfig bean doesn't need scanning here — it's registered
// automatically via ftgo-common's own Spring Boot auto-configuration. This service never uses
// OutboxPublisher/OutboxEventRepository since it never publishes, only ProcessedEventRepository
// for consume-side dedup — but com.sanjay.ftgo.common.outbox can't be scanned selectively at
// the class level, so scanning it for ProcessedEvent unavoidably also registers OutboxEvent,
// creating an unused outbox_events table as a harmless side effect.)
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.orderhistory.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.orderhistory.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
