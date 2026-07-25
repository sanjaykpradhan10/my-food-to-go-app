package com.sanjay.ftgo.accounting.api;

import com.sanjay.ftgo.accounting.domain.AuthorizationRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/authorizations")
public class AuthorizationController {

    private final AuthorizationRepository authorizationRepository;

    public AuthorizationController(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<AuthorizationInfo> viewByOrderId(@PathVariable Long orderId) {
        return authorizationRepository.findByOrderId(orderId)
                .map(authorization -> new AuthorizationInfo(
                        authorization.getId(), authorization.getOrderId(), authorization.getStatus().name()))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
