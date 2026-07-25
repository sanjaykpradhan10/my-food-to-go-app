package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KitchenServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private KitchenServiceProxy kitchenServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8090);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/tickets/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"ACCEPTED","readyBy":"2026-07-20T18:00:00Z"}
                                """)));

        SectionResult<TicketInfo> result = kitchenServiceProxy.findTicket(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<TicketInfo>) result).data().status()).isEqualTo("ACCEPTED");
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/tickets/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<TicketInfo> result = kitchenServiceProxy.findTicket(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<TicketInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = kitchenServiceProxy.findTicket(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
