package com.sanjay.ftgo.order.infrastructure;

import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    // Attaches order-service's own client_credentials bearer token to every outbound call this
    // service makes to other services' internal read endpoints - those endpoints require an
    // authenticated principal (Task 4) and order-service's proxies have no end-user token to
    // forward, since they're invoked from saga/query code paths, not directly from a request.
    @Bean
    @LoadBalanced
    public RestClient.Builder loadBalancedRestClientBuilder(ServiceTokenClient serviceTokenClient) {
        return RestClient.builder()
                .requestInterceptor((request, body, execution) -> {
                    request.getHeaders().setBearerAuth(serviceTokenClient.currentToken());
                    return execution.execute(request, body);
                });
    }

    @Bean
    public RestClient restaurantServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return loadBalancedRestClientBuilder
                .baseUrl("http://ftgo-restaurant-service")
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public RestClient kitchenServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return loadBalancedRestClientBuilder
                .baseUrl("http://ftgo-kitchen-service")
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public RestClient accountingServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return loadBalancedRestClientBuilder
                .baseUrl("http://ftgo-accounting-service")
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public RestClient deliveryServiceRestClient(@LoadBalanced RestClient.Builder loadBalancedRestClientBuilder) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return loadBalancedRestClientBuilder
                .baseUrl("http://ftgo-delivery-service")
                .requestFactory(requestFactory)
                .build();
    }
}
