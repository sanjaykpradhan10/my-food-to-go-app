package com.sanjay.ftgo.authserver.passwordgrant;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.AuthorizationGrantType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class OAuth2ResourceOwnerPasswordAuthenticationToken extends AbstractAuthenticationToken {

    public static final AuthorizationGrantType PASSWORD = new AuthorizationGrantType("password");

    private final Authentication clientPrincipal;
    private final String username;
    private final String password;
    private final Set<String> scopes;

    public OAuth2ResourceOwnerPasswordAuthenticationToken(Authentication clientPrincipal, String username,
            String password, Set<String> scopes) {
        super(Collections.emptyList());
        this.clientPrincipal = clientPrincipal;
        this.username = username;
        this.password = password;
        this.scopes = scopes;
        setAuthenticated(false);
    }

    @Override
    public Object getPrincipal() {
        return clientPrincipal;
    }

    @Override
    public Object getCredentials() {
        return "";
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Set<String> getScopes() {
        return scopes;
    }

    public Map<String, Object> getAdditionalParameters() {
        return Collections.emptyMap();
    }
}
