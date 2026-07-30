package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@AutoConfiguration
@EnableConfigurationProperties(GatewayApiKeyProperties.class)
public class GatewayCommonAutoConfiguration {
}
