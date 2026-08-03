package com.sanjay.ftgo.authserver.passwordgrant;

import com.sanjay.ftgo.authserver.FtgoUser;
import com.sanjay.ftgo.authserver.FtgoUserDetailsService;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.OAuth2TokenType;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2AccessTokenAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2ClientAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.context.AuthorizationServerContextHolder;
import org.springframework.security.oauth2.server.authorization.token.DefaultOAuth2TokenContext;
import org.springframework.security.oauth2.server.authorization.token.OAuth2TokenContext;
import org.springframework.security.oauth2.server.authorization.token.OAuth2TokenGenerator;

import java.security.Principal;
import java.util.List;

public class OAuth2ResourceOwnerPasswordAuthenticationProvider implements AuthenticationProvider {

    private final OAuth2AuthorizationService authorizationService;
    private final OAuth2TokenGenerator<?> tokenGenerator;
    private final FtgoUserDetailsService userDetailsService;
    private final PasswordEncoder passwordEncoder;

    public OAuth2ResourceOwnerPasswordAuthenticationProvider(OAuth2AuthorizationService authorizationService,
            OAuth2TokenGenerator<?> tokenGenerator, FtgoUserDetailsService userDetailsService,
            PasswordEncoder passwordEncoder) {
        this.authorizationService = authorizationService;
        this.tokenGenerator = tokenGenerator;
        this.userDetailsService = userDetailsService;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        OAuth2ResourceOwnerPasswordAuthenticationToken grant =
                (OAuth2ResourceOwnerPasswordAuthenticationToken) authentication;
        OAuth2ClientAuthenticationToken clientPrincipal =
                (OAuth2ClientAuthenticationToken) grant.getPrincipal();
        RegisteredClient registeredClient = clientPrincipal.getRegisteredClient();

        FtgoUser user = userDetailsService.findByUsername(grant.getUsername())
                .orElseThrow(() -> new BadCredentialsException("Bad credentials"));
        if (!passwordEncoder.matches(grant.getPassword(), user.password())) {
            throw new BadCredentialsException("Bad credentials");
        }

        // sub = the user's numeric id (as a string), not the username — Order Service's ACL
        // check compares this directly against Order.consumerId.
        Principal resourceOwner = () -> String.valueOf(user.id());

        // JwtGenerator only copies a fixed set of OAuth2TokenContext fields (registeredClient,
        // principal, authorizationServerContext, authorizedScopes, tokenType,
        // authorizationGrantType, authorizationGrant) into the JwtEncodingContext it hands to
        // the customizer — arbitrary .put() attributes on this builder do NOT survive that copy.
        // So the role rides on the principal's authorities instead, which IS copied, and the
        // same principal is stashed on the OAuth2Authorization below so a later refresh_token
        // grant (which rebuilds its context from the saved authorization) still carries it.
        Authentication principal = new UsernamePasswordAuthenticationToken(
                resourceOwner.getName(), null, List.of(new SimpleGrantedAuthority("ROLE_" + user.role())));

        DefaultOAuth2TokenContext.Builder contextBuilder = DefaultOAuth2TokenContext.builder()
                .registeredClient(registeredClient)
                .principal(principal)
                .authorizationServerContext(AuthorizationServerContextHolder.getContext())
                .authorizedScopes(grant.getScopes())
                .authorizationGrantType(OAuth2ResourceOwnerPasswordAuthenticationToken.PASSWORD)
                .authorizationGrant(grant);

        OAuth2TokenContext accessTokenContext = contextBuilder
                .tokenType(OAuth2TokenType.ACCESS_TOKEN)
                .build();
        var generatedAccessToken = tokenGenerator.generate(accessTokenContext);
        if (generatedAccessToken == null) {
            throw new BadCredentialsException("Failed to generate access token");
        }
        OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER,
                generatedAccessToken.getTokenValue(), generatedAccessToken.getIssuedAt(),
                generatedAccessToken.getExpiresAt());

        OAuth2Authorization.Builder authorizationBuilder = OAuth2Authorization.withRegisteredClient(registeredClient)
                .principalName(resourceOwner.getName())
                .authorizationGrantType(OAuth2ResourceOwnerPasswordAuthenticationToken.PASSWORD)
                .authorizedScopes(grant.getScopes())
                // Read by OAuth2RefreshTokenAuthenticationProvider (the built-in refresh_token
                // grant) to rebuild the principal for the next access token it mints.
                .attribute(Principal.class.getName(), principal)
                .token(accessToken);

        OAuth2TokenContext refreshTokenContext = contextBuilder
                .tokenType(OAuth2TokenType.REFRESH_TOKEN)
                .build();
        var generatedRefreshToken = tokenGenerator.generate(refreshTokenContext);
        OAuth2RefreshToken refreshToken = null;
        if (generatedRefreshToken instanceof OAuth2RefreshToken rt) {
            refreshToken = rt;
            authorizationBuilder.refreshToken(refreshToken);
        }

        OAuth2Authorization authorization = authorizationBuilder.build();
        authorizationService.save(authorization);

        return new OAuth2AccessTokenAuthenticationToken(registeredClient, clientPrincipal, accessToken,
                refreshToken);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return OAuth2ResourceOwnerPasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
