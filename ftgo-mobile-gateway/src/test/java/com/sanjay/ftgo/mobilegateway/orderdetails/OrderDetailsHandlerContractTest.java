package com.sanjay.ftgo.mobilegateway.orderdetails;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.cloud.contract.stubrunner.junit.StubRunnerExtension;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

// Consumer-side contract test: runs a real WireMock instance generated from
// ftgo-order-service-contracts' shouldReturnOrderById.groovy (Task 2) and verifies
// OrderDetailsHandler correctly parses what that contract says the real provider returns,
// instead of asserting against a hand-rolled mock that could silently drift from the contract.
//
// StubRunnerExtension (not @AutoConfigureStubRunner) is used deliberately: the
// @AutoConfigureStubRunner annotation only activates via a Spring TestContext (e.g.
// @SpringBootTest), and this test intentionally constructs OrderDetailsHandler and its
// WebClients by hand rather than bootstrapping a full application context - the plain JUnit 5
// extension starts the same WireMock stub server without that context.
class OrderDetailsHandlerContractTest {

    @RegisterExtension
    static StubRunnerExtension stubRunner = new StubRunnerExtension()
            .downloadStub("com.sanjay.ftgo", "ftgo-order-service-contracts")
            .stubsMode(StubRunnerProperties.StubsMode.LOCAL);

    @Test
    void fetchOrderDetailsParsesOrderServiceSectionPerContract() throws Exception {
        // findStubUrl resolves the actual host:port StubRunnerExtension started the WireMock
        // instance on (a random free port unless explicitly pinned) - hardcoding a port here
        // would be a source of flaky collisions across parallel test runs.
        String stubBaseUrl = stubRunner.findStubUrl("com.sanjay.ftgo", "ftgo-order-service-contracts").toString();
        WebClient orderServiceClient = WebClient.create(stubBaseUrl);
        // The kitchen/accounting/delivery backends have no contract or stub in this test - this
        // is a REST contract test for the order-service section only (per Task 3's scoping
        // guidance). Port 1 is a reserved, always-unreachable TCP port, so these three sections
        // deterministically resolve to SectionResult.Unavailable via the handler's own
        // onErrorResume/circuit-breaker fallback rather than hanging or asserting anything about
        // those backends' actual (unmodeled) contracts.
        WebClient unreachableClient = WebClient.create("http://localhost:1");
        BackendClients clients = new BackendClients(
                orderServiceClient, unreachableClient, unreachableClient, unreachableClient);

        // spring-cloud-commons 4.3.x (this project's pinned Spring Cloud 2025.0.3 train) ships no
        // ready-made no-op ReactiveCircuitBreakerFactory, so this passthrough runs the wrapped
        // Mono directly with no open/half-open state machine - sufficient here since this test
        // isn't exercising circuit-breaker behavior, only the order-service response parsing the
        // contract governs.
        OrderDetailsHandler handler = new OrderDetailsHandler(clients, new PassthroughReactiveCircuitBreakerFactory());

        OrderDetails orderDetails = handler.fetchOrderDetails(1223232L)
                .block(Duration.ofSeconds(5));

        // The contract's example order id is 1223232 (Task 2, Step 3) - verifying the order
        // section came back Found with exactly the contract's JSON body confirms this consumer
        // correctly parses what the real provider (per the shared contract) actually returns.
        assertInstanceOf(SectionResult.Found.class, orderDetails.order());
        String orderJson = ((SectionResult.Found<String>) orderDetails.order()).data();

        // Parse-and-assert-on-fields rather than a raw string-equals, so this test isn't
        // brittle to incidental JSON key ordering from the stub's response serialization.
        JsonNode order = new ObjectMapper().readTree(orderJson);
        assertEquals(1223232, order.get("id").asLong());
        assertEquals(1, order.get("consumerId").asLong());
        assertEquals(1, order.get("restaurantId").asLong());
        assertEquals("APPROVAL_PENDING", order.get("status").asText());
        assertEquals(1, order.get("lineItems").size());
        assertEquals(10, order.get("lineItems").get(0).get("menuItemId").asLong());
        assertEquals(2, order.get("lineItems").get(0).get("quantity").asLong());
    }

    private static class PassthroughReactiveCircuitBreakerFactory
            extends ReactiveCircuitBreakerFactory<Object, org.springframework.cloud.client.circuitbreaker.ConfigBuilder<Object>> {
        @Override
        public ReactiveCircuitBreaker create(String id) {
            return new ReactiveCircuitBreaker() {
                @Override
                public <T> Mono<T> run(Mono<T> toRun, Function<Throwable, Mono<T>> fallback) {
                    return toRun.onErrorResume(fallback);
                }

                @Override
                public <T> reactor.core.publisher.Flux<T> run(
                        reactor.core.publisher.Flux<T> toRun, Function<Throwable, reactor.core.publisher.Flux<T>> fallback) {
                    return toRun.onErrorResume(fallback);
                }
            };
        }

        @Override
        public org.springframework.cloud.client.circuitbreaker.ConfigBuilder<Object> configBuilder(String id) {
            throw new UnsupportedOperationException("not used by this test");
        }

        @Override
        public void configureDefault(
                Function<String, Object> defaultConfiguration) {
            // no-op: this passthrough factory ignores circuit breaker configuration
        }
    }
}
