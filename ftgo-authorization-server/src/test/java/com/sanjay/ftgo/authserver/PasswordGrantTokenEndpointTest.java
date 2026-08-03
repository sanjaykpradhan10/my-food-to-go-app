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
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PasswordGrantTokenEndpointTest {

    @LocalServerPort
    private int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate("ftgo-gateway", "gateway-secret");

    @Test
    void shouldIssueAccessTokenForValidCredentials() throws Exception {
        ResponseEntity<Map> response = requestToken("consumer1", "password");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> body = response.getBody();
        assertThat(body).isNotNull();
        String accessToken = (String) body.get("access_token");
        assertThat(accessToken).isNotBlank();

        SignedJWT jwt = (SignedJWT) JWTParser.parse(accessToken);
        JWTClaimsSet claims = jwt.getJWTClaimsSet();
        assertThat(claims.getSubject()).isEqualTo("1");
        assertThat(claims.getStringListClaim("roles")).containsExactly("CONSUMER");
    }

    @Test
    void shouldExposeJwks() {
        ResponseEntity<Map> response = restTemplate.getForEntity("http://localhost:" + port + "/oauth2/jwks", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsKey("keys");
        assertThat((List<?>) response.getBody().get("keys")).isNotEmpty();
    }

    @Test
    void shouldExposeJwksWithoutClientAuthentication() {
        // Gateways and other services fetch the JWK Set anonymously to verify tokens;
        // this must succeed with no Basic-Auth client credentials at all, unlike
        // shouldExposeJwks() above which (incidentally) always sends them.
        RestTemplate anonymousRestTemplate = new RestTemplate();

        ResponseEntity<Map> response = anonymousRestTemplate.getForEntity(
                "http://localhost:" + port + "/oauth2/jwks", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsKey("keys");
        assertThat((List<?>) response.getBody().get("keys")).isNotEmpty();
    }

    @Test
    void shouldRejectBadCredentials() {
        ResponseEntity<Map> response = requestToken("consumer1", "wrong");

        assertThat(response.getStatusCode().value()).isIn(400, 401);
    }

    @Test
    void shouldRetainRolesClaimAcrossRefreshTokenGrant() throws Exception {
        // The whole point of stashing role info as an OAuth2Authorization attribute
        // (see OAuth2ResourceOwnerPasswordAuthenticationProvider) is that it survives
        // Spring Authorization Server's built-in refresh-token grant, not just the
        // initial password grant. Exercise that grant explicitly.
        ResponseEntity<Map> initialResponse = requestToken("consumer1", "password");
        assertThat(initialResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> initialBody = initialResponse.getBody();
        assertThat(initialBody).isNotNull();
        String refreshToken = (String) initialBody.get("refresh_token");
        assertThat(refreshToken).isNotBlank();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "refresh_token");
        form.add("refresh_token", refreshToken);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(form, headers);
        ResponseEntity<Map> refreshResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/oauth2/token", request, Map.class);

        assertThat(refreshResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> refreshBody = refreshResponse.getBody();
        assertThat(refreshBody).isNotNull();
        String refreshedAccessToken = (String) refreshBody.get("access_token");
        assertThat(refreshedAccessToken).isNotBlank();

        SignedJWT jwt = (SignedJWT) JWTParser.parse(refreshedAccessToken);
        JWTClaimsSet claims = jwt.getJWTClaimsSet();
        assertThat(claims.getSubject()).isEqualTo("1");
        assertThat(claims.getStringListClaim("roles")).containsExactly("CONSUMER");
    }

    private ResponseEntity<Map> requestToken(String username, String password) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("username", username);
        form.add("password", password);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(form, headers);
        return restTemplate.postForEntity("http://localhost:" + port + "/oauth2/token", request, Map.class);
    }
}
