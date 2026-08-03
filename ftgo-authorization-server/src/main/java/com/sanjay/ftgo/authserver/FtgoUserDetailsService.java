package com.sanjay.ftgo.authserver;

import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Hardcoded seed users for this dev/learning project — no registration flow, no persistence.
 * IDs are chosen to line up with ftgo-end-to-end-test's existing Cucumber scenarios: consumer1's
 * id (1) matches the auto-increment id ftgo-consumer-service assigns the first consumer created
 * against a fresh database, which is exactly what PlaceReviseCancelOrder.feature's "an active
 * consumer" step does on every e2e run.
 */
@Component
public class FtgoUserDetailsService {

    private final List<FtgoUser> users;

    public FtgoUserDetailsService(PasswordEncoder passwordEncoder) {
        String encoded = passwordEncoder.encode("password");
        this.users = List.of(
                new FtgoUser(1, "consumer1", encoded, "CONSUMER"),
                new FtgoUser(5, "consumer2", encoded, "CONSUMER"),
                new FtgoUser(2, "restaurant1", encoded, "RESTAURANT"),
                new FtgoUser(3, "courier1", encoded, "COURIER"),
                new FtgoUser(4, "admin1", encoded, "ADMIN"));
    }

    public Optional<FtgoUser> findByUsername(String username) {
        return users.stream().filter(u -> u.username().equals(username)).findFirst();
    }
}
