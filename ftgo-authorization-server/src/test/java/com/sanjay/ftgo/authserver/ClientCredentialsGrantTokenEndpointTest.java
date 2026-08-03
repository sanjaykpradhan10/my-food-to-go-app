package com.sanjay.ftgo.authserver;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

// Exercises order-service's service-identity token: no end user, so the request is
// authenticated purely via the client's own Basic-Auth credentials (see
// AuthorizationServerConfig.registeredClientRepository's "ftgo-order-service" client).
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClientCredentialsGrantTokenEndpointTest {

    @LocalServerPort
    private int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate("ftgo-order-service", "order-service-secret");

    @Test
    void shouldIssueServiceRoleTokenForClientCredentials() throws Exception {
        ResponseEntity<Map> response = requestToken();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> body = response.getBody();
        assertThat(body).isNotNull();
        String accessToken = (String) body.get("access_token");
        assertThat(accessToken).isNotBlank();

        SignedJWT jwt = (SignedJWT) JWTParser.parse(accessToken);
        JWTClaimsSet claims = jwt.getJWTClaimsSet();
        assertThat(claims.getStringListClaim("roles")).containsExactly("SERVICE");
    }

    @Test
    void shouldRejectClientCredentialsWithBadSecret() {
        TestRestTemplate badRestTemplate = new TestRestTemplate("ftgo-order-service", "wrong-secret");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "client_credentials");
        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(form, headers);

        ResponseEntity<Map> response = badRestTemplate.postForEntity(
                "http://localhost:" + port + "/oauth2/token", request, Map.class);

        assertThat(response.getStatusCode().value()).isIn(400, 401);
    }

    private ResponseEntity<Map> requestToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "client_credentials");

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(form, headers);
        return restTemplate.postForEntity("http://localhost:" + port + "/oauth2/token", request, Map.class);
    }
}
