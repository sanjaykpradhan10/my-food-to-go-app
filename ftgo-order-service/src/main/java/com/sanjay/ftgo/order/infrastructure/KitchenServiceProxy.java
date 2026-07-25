package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class KitchenServiceProxy implements KitchenServicePort {

    private final RestClient restClient;

    public KitchenServiceProxy(RestClient kitchenServiceRestClient) {
        this.restClient = kitchenServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "kitchenService", fallbackMethod = "findTicketFallback")
    public SectionResult<TicketInfo> findTicket(Long orderId) {
        try {
            TicketInfo info = restClient.get()
                    .uri("/tickets/order/{orderId}", orderId)
                    .retrieve()
                    .body(TicketInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<TicketInfo> findTicketFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
