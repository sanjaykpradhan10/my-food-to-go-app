package com.sanjay.ftgo.common.audit;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

// Registers AuditLoggingAspect for any service that depends on ftgo-common, the same
// auto-configuration pattern OutboxAutoConfiguration uses for OutboxPublisher - no per-service
// @ComponentScan or @EnableAspectJAutoProxy needed.
@AutoConfiguration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = "com.sanjay.ftgo.common.audit")
public class AuditLoggingAutoConfiguration {
}
