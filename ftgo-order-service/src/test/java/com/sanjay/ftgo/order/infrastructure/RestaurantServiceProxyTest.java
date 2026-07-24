package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.Found;
import com.sanjay.ftgo.order.domain.NotFound;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.Unavailable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.beans.factory.annotation.Autowired;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RestaurantServiceProxyTest {

    private WireMockServer wireMockServer;

    @Autowired
    private RestaurantServiceProxy restaurantServiceProxy;

    @BeforeEach
    void startWireMock() {
        wireMockServer = new WireMockServer(8089);
        wireMockServer.start();
    }

    @AfterEach
    void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    void returnsRestaurantInfoOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/restaurants/1"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"name":"Ajanta Indian Cuisine","menuItems":[{"id":10,"name":"Chicken Tikka Masala","price":14.99}]}
                                """)));

        RestaurantInfo result = restaurantServiceProxy.findRestaurant(1L);

        assertThat(result.id()).isEqualTo(1L);
        assertThat(result.name()).isEqualTo("Ajanta Indian Cuisine");
        assertThat(result.menuItems()).hasSize(1);
    }

    @Test
    void throwsRestaurantNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/restaurants/99"))
                .willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> restaurantServiceProxy.findRestaurant(99L))
                .isInstanceOf(RestaurantNotFoundException.class);
    }

    @Test
    void tripsCircuitBreakerAfterRepeatedFailures() {
        wireMockServer.stop();

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> restaurantServiceProxy.findRestaurant(1L))
                    .isInstanceOf(RestaurantServiceUnavailableException.class);
        }

        wireMockServer.start();
        wireMockServer.stubFor(get(urlEqualTo("/restaurants/1"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"name":"Ajanta Indian Cuisine","menuItems":[]}
                                """)));

        assertThatThrownBy(() -> restaurantServiceProxy.findRestaurant(1L))
                .isInstanceOf(RestaurantServiceUnavailableException.class);
    }

    @Test
    void findRestaurantForViewReturnsFoundOnSuccess() {
        wireMockServer.stubFor(get(urlEqualTo("/restaurants/1"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"id":1,"name":"Ajanta Indian Cuisine","menuItems":[]}
                                """)));

        SectionResult<RestaurantInfo> result = restaurantServiceProxy.findRestaurantForView(1L);

        assertThat(result).isInstanceOf(Found.class);
        assertThat(((Found<RestaurantInfo>) result).data().name()).isEqualTo("Ajanta Indian Cuisine");
    }

    @Test
    void findRestaurantForViewReturnsNotFoundOn404() {
        wireMockServer.stubFor(get(urlEqualTo("/restaurants/99"))
                .willReturn(aResponse().withStatus(404)));

        SectionResult<RestaurantInfo> result = restaurantServiceProxy.findRestaurantForView(99L);

        assertThat(result).isInstanceOf(NotFound.class);
    }

    @Test
    void findRestaurantForViewReturnsUnavailableWhenCircuitOpen() {
        wireMockServer.stop();

        SectionResult<RestaurantInfo> result = null;
        for (int i = 0; i < 4; i++) {
            result = restaurantServiceProxy.findRestaurantForView(1L);
        }

        assertThat(result).isInstanceOf(Unavailable.class);
    }
}
