package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.api-key")
public class GatewayApiKeyProperties {

    private final String value;

    public GatewayApiKeyProperties(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
