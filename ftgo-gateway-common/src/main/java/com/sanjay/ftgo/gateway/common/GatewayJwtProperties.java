package com.sanjay.ftgo.gateway.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.jwt")
public class GatewayJwtProperties {

    private final String jwkSetUri;

    public GatewayJwtProperties(String jwkSetUri) {
        this.jwkSetUri = jwkSetUri;
    }

    public String jwkSetUri() {
        return jwkSetUri;
    }
}
