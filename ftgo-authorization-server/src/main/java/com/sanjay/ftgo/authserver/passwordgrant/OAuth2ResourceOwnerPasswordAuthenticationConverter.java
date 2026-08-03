package com.sanjay.ftgo.authserver.passwordgrant;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.AuthenticationConverter;

import java.util.Collections;

public class OAuth2ResourceOwnerPasswordAuthenticationConverter implements AuthenticationConverter {

    @Override
    public Authentication convert(HttpServletRequest request) {
        String grantType = request.getParameter("grant_type");
        if (!"password".equals(grantType)) {
            return null;
        }
        Authentication clientPrincipal = SecurityContextHolder.getContext().getAuthentication();
        String username = request.getParameter("username");
        String password = request.getParameter("password");
        return new OAuth2ResourceOwnerPasswordAuthenticationToken(clientPrincipal, username, password,
                Collections.<String>emptySet());
    }
}
