package com.sanjay.ftgo.common.outbox;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

// Registers OutboxPublisher and KafkaProducerConfig for any service that depends on
// ftgo-common, via Spring Boot's auto-configuration mechanism (see
// META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports).
// Before this, every consuming service needed its own @ComponentScan of
// com.sanjay.ftgo.common.outbox — omitting it silently disabled the outbox poller with no
// startup error (the bug fixed earlier in this same change). Auto-configuration makes that
// failure mode structurally impossible: any service that adds ftgo-common to its classpath
// gets these beans automatically, with nothing to remember or copy-paste.
@AutoConfiguration
@ComponentScan(basePackages = "com.sanjay.ftgo.common.outbox")
public class OutboxAutoConfiguration {
}
