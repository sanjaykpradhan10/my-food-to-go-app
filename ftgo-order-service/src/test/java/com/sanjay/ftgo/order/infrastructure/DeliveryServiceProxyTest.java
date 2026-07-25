package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
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
class DeliveryServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private DeliveryServiceProxy deliveryServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8092);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/deliveries/order/42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"orderId":42,"status":"SCHEDULED","courierId":3}
                                """)));

        SectionResult<DeliveryInfo> result = deliveryServiceProxy.findDelivery(42L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<DeliveryInfo>) result).data().courierId()).isEqualTo(3L);
    }

    @Test
    void returnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/deliveries/order/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<DeliveryInfo> result = deliveryServiceProxy.findDelivery(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void returnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<DeliveryInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = deliveryServiceProxy.findDelivery(42L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
