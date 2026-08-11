package com.sanjay.ftgo.auditlog.config;

import org.springframework.context.annotation.Configuration;

// Deliberately empty: unlike ftgo-order-history-service (4 listeners racing to update the same
// order_views row, needing a custom retrying container factory - see that service's
// KafkaConsumerConfig), this service has a single @KafkaListener performing plain inserts with
// no shared-row contention, so Boot's default listener container factory is sufficient.
@Configuration
public class KafkaConsumerConfig {
}
