package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class AccountingServiceProxy implements AccountingServicePort {

    private final RestClient restClient;

    public AccountingServiceProxy(RestClient accountingServiceRestClient) {
        this.restClient = accountingServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "accountingService", fallbackMethod = "findAuthorizationFallback")
    public SectionResult<AuthorizationInfo> findAuthorization(Long orderId) {
        try {
            AuthorizationInfo info = restClient.get()
                    .uri("/authorizations/order/{orderId}", orderId)
                    .retrieve()
                    .body(AuthorizationInfo.class);
            return new Found<>(info);
        } catch (HttpClientErrorException.NotFound e) {
            return new NotFound<>();
        }
    }

    @SuppressWarnings("unused")
    private SectionResult<AuthorizationInfo> findAuthorizationFallback(Long orderId, Throwable throwable) {
        return new Unavailable<>(throwable.getMessage());
    }
}
