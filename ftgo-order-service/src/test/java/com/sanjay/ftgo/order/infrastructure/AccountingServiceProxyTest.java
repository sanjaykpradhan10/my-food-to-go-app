package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AccountingServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private AccountingServiceProxy accountingServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8091);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/authorizations/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"AUTHORIZED"}
                                """)));

        SectionResult<AuthorizationInfo> result = accountingServiceProxy.findAuthorization(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<AuthorizationInfo>) result).data().status()).isEqualTo("AUTHORIZED");
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/authorizations/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<AuthorizationInfo> result = accountingServiceProxy.findAuthorization(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<AuthorizationInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = accountingServiceProxy.findAuthorization(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
