package com.sanjay.ftgo.delivery.infrastructure;

import com.sanjay.ftgo.delivery.domain.Courier;
import com.sanjay.ftgo.delivery.domain.CourierRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CourierSeeder implements CommandLineRunner {

    private final CourierRepository courierRepository;

    public CourierSeeder(CourierRepository courierRepository) {
        this.courierRepository = courierRepository;
    }

    // Seeded courier pool must outlast a single clean e2e run: OrderAccessControl.feature's four
    // scenarios each place an order but never cancel it (they're testing access control, not the
    // order lifecycle), so their couriers are never released back to the pool. Combined with
    // PlaceReviseCancelOrder.feature's one order, a single e2eTest run needs 5 couriers - 3 was
    // exhausted well before that, causing a legitimate-looking but spurious "no courier available"
    // DeliverySchedulingFailed once order-service's 401 blocker (Task 6b) stopped masking it.
    private static final String[] COURIER_NAMES = {
            "Alex", "Bailey", "Casey", "Drew", "Emerson", "Frankie", "Gray", "Harper", "Indigo", "Jordan"
    };

    @Override
    public void run(String... args) {
        if (courierRepository.count() > 0) {
            return;
        }
        for (String name : COURIER_NAMES) {
            courierRepository.save(new Courier(name));
        }
    }
}
