package com.sanjay.ftgo.mobilegateway.orderdetails;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;

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
        handler = new OrderDetailsHandler(clients);
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
