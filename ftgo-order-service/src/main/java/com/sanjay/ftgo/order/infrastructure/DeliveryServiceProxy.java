package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class DeliveryServiceProxy implements DeliveryServicePort {

    private final RestClient restClient;

    public DeliveryServiceProxy(RestClient deliveryServiceRestClient) {
        this.restClient = deliveryServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "deliveryService", fallbackMethod = "findDeliveryFallback")
    public SectionResult<DeliveryInfo> findDelivery(Long orderId) {
        try {
            DeliveryInfo info = restClient.get()
                    .uri("/deliveries/order/{orderId}", orderId)
                    .retrieve()
                    .body(DeliveryInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<DeliveryInfo> findDeliveryFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
