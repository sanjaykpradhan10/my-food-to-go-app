package com.sanjay.ftgo.kitchen.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EntityScan(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
@EnableJpaRepositories(basePackages = {"com.sanjay.ftgo.kitchen.domain", "com.sanjay.ftgo.common.outbox"})
public class PersistenceConfig {
}
