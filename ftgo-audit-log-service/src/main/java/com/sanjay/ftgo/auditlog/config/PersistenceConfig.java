package com.sanjay.ftgo.auditlog.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

// Kept separate from FtgoAuditLogServiceApplication - see ftgo-order-history-service's
// PersistenceConfig for why @EntityScan/@EnableJpaRepositories must not sit directly on the
// @SpringBootApplication class (breaks @WebMvcTest slice filtering).
@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.auditlog.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.auditlog.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
