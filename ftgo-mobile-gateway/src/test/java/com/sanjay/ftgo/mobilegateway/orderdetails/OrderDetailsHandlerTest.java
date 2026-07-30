package com.sanjay.ftgo.mobilegateway.orderdetails;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class OrderDetailsHandlerTest {

    private MockWebServer orderServer;
    private MockWebServer kitchenServer;
    private MockWebServer accountingServer;
    private MockWebServer deliveryServer;
    private OrderDetailsHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        orderServer = new MockWebServer();
        kitchenServer = new MockWebServer();
        accountingServer = new MockWebServer();
        deliveryServer = new MockWebServer();
        orderServer.start();
        kitchenServer.start();
        accountingServer.start();
        deliveryServer.start();

        BackendClients clients = new BackendClients(
                WebClient.builder().baseUrl(orderServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(kitchenServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(accountingServer.url("/").toString()).build(),
                WebClient.builder().baseUrl(deliveryServer.url("/").toString()).build());

        // Real (not mocked) reactive circuit breaker factory with a short default timeout, so
        // the test exercises the actual breaker wiring rather than assuming it's a no-op.
        var circuitBreakerFactory = new ReactiveResilience4JCircuitBreakerFactory(
                CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults());
        circuitBreakerFactory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.ofDefaults())
                .timeLimiterConfig(io.github.resilience4j.timelimiter.TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(3))
                        .build())
                .build());

        handler = new OrderDetailsHandler(clients, circuitBreakerFactory);
    }

    @AfterEach
    void tearDown() throws IOException {
        orderServer.shutdown();
        kitchenServer.shutdown();
        accountingServer.shutdown();
        deliveryServer.shutdown();
    }

    @Test
    void composesAllFourSectionsWhenAllServicesRespond() {
        orderServer.enqueue(new MockResponse().setBody("{\"id\":1}").addHeader("Content-Type", "application/json"));
        kitchenServer.enqueue(new MockResponse().setBody("{\"ticketId\":1}").addHeader("Content-Type", "application/json"));
        accountingServer.enqueue(new MockResponse().setBody("{\"status\":\"AUTHORIZED\"}").addHeader("Content-Type", "application/json"));
        deliveryServer.enqueue(new MockResponse().setBody("{\"status\":\"SCHEDULED\"}").addHeader("Content-Type", "application/json"));

        OrderDetails result = handler.fetchOrderDetails(1L).block();

        assertThat(result.order()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.ticket()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.authorization()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.delivery()).isInstanceOf(SectionResult.Found.class);
    }

    @Test
    void fetchesAllFourSectionsConcurrentlyNotSequentially() {
        // Each backend delays its response by 500ms. If fetchOrderDetails() chained the four
        // calls sequentially instead of using Mono.zip to fire them concurrently, the total
        // would be >= 2000ms; concurrent execution keeps it close to a single 500ms delay.
        Duration perCallDelay = Duration.ofMillis(500);
        orderServer.enqueue(new MockResponse().setBody("{\"id\":1}")
                .addHeader("Content-Type", "application/json").setBodyDelay(perCallDelay.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
        kitchenServer.enqueue(new MockResponse().setBody("{\"ticketId\":1}")
                .addHeader("Content-Type", "application/json").setBodyDelay(perCallDelay.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
        accountingServer.enqueue(new MockResponse().setBody("{\"status\":\"AUTHORIZED\"}")
                .addHeader("Content-Type", "application/json").setBodyDelay(perCallDelay.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
        deliveryServer.enqueue(new MockResponse().setBody("{\"status\":\"SCHEDULED\"}")
                .addHeader("Content-Type", "application/json").setBodyDelay(perCallDelay.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));

        long start = System.nanoTime();
        OrderDetails result = handler.fetchOrderDetails(1L).block();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(result.order()).isInstanceOf(SectionResult.Found.class);
        assertThat(elapsed).isLessThan(perCallDelay.multipliedBy(2));
    }

    @Test
    void degradesOneSectionWhenThatServiceReturns404WithoutFailingTheWholeRequest() {
        orderServer.enqueue(new MockResponse().setBody("{\"id\":1}").addHeader("Content-Type", "application/json"));
        kitchenServer.enqueue(new MockResponse().setResponseCode(404));
        accountingServer.enqueue(new MockResponse().setBody("{\"status\":\"AUTHORIZED\"}").addHeader("Content-Type", "application/json"));
        deliveryServer.enqueue(new MockResponse().setBody("{\"status\":\"SCHEDULED\"}").addHeader("Content-Type", "application/json"));

        OrderDetails result = handler.fetchOrderDetails(1L).block();

        assertThat(result.order()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.ticket()).isInstanceOf(SectionResult.NotFound.class);
        assertThat(result.authorization()).isInstanceOf(SectionResult.Found.class);
        assertThat(result.delivery()).isInstanceOf(SectionResult.Found.class);
    }
}
