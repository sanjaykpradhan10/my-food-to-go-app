package com.sanjay.ftgo.order.contracttest;

import com.sanjay.ftgo.order.api.OrderController;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderRepository;
import io.restassured.module.mockmvc.RestAssuredMockMvc;
import org.junit.jupiter.api.BeforeEach;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Base class the Spring Cloud Contract Gradle plugin's generated provider test
// (from ftgo-order-service-contracts' shouldReturnOrderById.groovy) extends at
// contractTest time. Only orderRepository is exercised by GET /orders/{id}, so the
// other four OrderController collaborators are null - this is a read-only lookup path.
public abstract class HttpBase {

    @BeforeEach
    public void setup() {
        OrderRepository orderRepository = mock(OrderRepository.class);
        Order order = ContractFixtures.sampleOrder();
        when(orderRepository.findById(ContractFixtures.ORDER_ID)).thenReturn(Optional.of(order));

        OrderController controller = new OrderController(null, null, null, null, orderRepository);
        RestAssuredMockMvc.standaloneSetup(controller);
    }
}
