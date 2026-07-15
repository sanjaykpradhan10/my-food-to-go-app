# Ch. 3 IPC — REST Call with Circuit Breaker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a synchronous REST call from `order-service` to `restaurant-service`, protected by a Resilience4j circuit breaker, as the first Ch. 3 IPC pattern in the FTGO app.

**Architecture:** `restaurant-service` owns `Restaurant`/`MenuItem` persisted via JPA and exposes `GET /restaurants/{id}`. `order-service` has an in-memory `Order` domain model and a hexagonal outbound port (`RestaurantServicePort`) implemented by an adapter (`RestaurantServiceProxy`) that calls restaurant-service via Spring `RestClient` wrapped in a Resilience4j `@CircuitBreaker`. `order-service` exposes `POST /orders`, which validates the restaurant and menu items before accepting the order.

**Tech Stack:** Java 21, Spring Boot 3.5.3, Spring Data JPA, MySQL 8.4 (H2 in tests), Spring `RestClient`, Resilience4j (`resilience4j-spring-boot3` + `spring-boot-starter-aop`), WireMock (order-service tests), JUnit 5, Mockito, Gradle 8.14.2 (multi-module).

## Global Constraints

- Java sourceCompatibility 21, Spring Boot 3.5.3 (from root `build.gradle`) — do not change these.
- Package roots: `com.sanjay.ftgo.restaurant` (restaurant-service), `com.sanjay.ftgo.order` (order-service) — matches existing `FtgoRestaurantServiceApplication` / `FtgoOrderServiceApplication`.
- Hexagonal packages per service: `domain`, `infrastructure`, `api` — matches existing empty package stubs.
- restaurant-service runs on port 8085, order-service on port 8082 (from existing `application.yml` files) — do not change.
- Test config already uses H2 in `MODE=MySQL` with `ddl-auto: create-drop` (`src/test/resources/application.yml` in each service) — reuse as-is, do not duplicate.
- Order persistence is out of scope for this plan — `Order` stays in-memory (per spec `docs/superpowers/specs/2026-07-15-ch3-rest-circuit-breaker-design.md`).
- Messaging/Kafka, transactional outbox, transaction log tailing, and service discovery are out of scope for this plan.

---

### Task 1: restaurant-service — domain model, JPA repository, seed data

**Files:**
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/Restaurant.java`
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/MenuItem.java`
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepository.java`
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/DataSeeder.java`
- Test: `ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepositoryTest.java`
- Delete: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/.gitkeep`
- Delete: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/.gitkeep`

**Interfaces:**
- Produces: `Restaurant(String name, List<MenuItem> menuItems)` constructor; `Restaurant.getId(): Long`, `getName(): String`, `getMenuItems(): List<MenuItem>`. `MenuItem(String name, BigDecimal price)` constructor; `MenuItem.getId(): Long`, `getName(): String`, `getPrice(): BigDecimal`. `RestaurantRepository extends JpaRepository<Restaurant, Long>`.

- [ ] **Step 1: Write the failing repository test**

Create `ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepositoryTest.java`:

```java
package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class RestaurantRepositoryTest {

    @Autowired
    private RestaurantRepository restaurantRepository;

    @Test
    void savesAndFindsRestaurantWithMenuItems() {
        Restaurant restaurant = new Restaurant("Ajanta Indian Cuisine", List.of(
                new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99")),
                new MenuItem("Garlic Naan", new BigDecimal("3.50"))
        ));

        Restaurant saved = restaurantRepository.save(restaurant);

        Optional<Restaurant> found = restaurantRepository.findById(saved.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Ajanta Indian Cuisine");
        assertThat(found.get().getMenuItems()).hasSize(2);
        assertThat(found.get().getMenuItems())
                .extracting(MenuItem::getName)
                .containsExactlyInAnyOrder("Chicken Tikka Masala", "Garlic Naan");
    }
}
```

- [ ] **Step 2: Run test to verify it fails to compile (domain classes don't exist yet)**

Run: `./gradlew :ftgo-restaurant-service:test --tests "com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepositoryTest"`
Expected: FAIL — compilation error, `Restaurant`/`MenuItem`/`RestaurantRepository` cannot be resolved.

- [ ] **Step 3: Create the `MenuItem` entity**

Delete `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/.gitkeep` and create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/MenuItem.java`:

```java
package com.sanjay.ftgo.restaurant.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "menu_items")
public class MenuItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private BigDecimal price;

    @ManyToOne
    @JoinColumn(name = "restaurant_id")
    private Restaurant restaurant;

    protected MenuItem() {
    }

    public MenuItem(String name, BigDecimal price) {
        this.name = name;
        this.price = price;
    }

    void setRestaurant(Restaurant restaurant) {
        this.restaurant = restaurant;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }
}
```

- [ ] **Step 4: Create the `Restaurant` entity**

Create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/Restaurant.java`:

```java
package com.sanjay.ftgo.restaurant.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.util.List;

@Entity
@Table(name = "restaurants")
public class Restaurant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    @OneToMany(mappedBy = "restaurant", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<MenuItem> menuItems;

    protected Restaurant() {
    }

    public Restaurant(String name, List<MenuItem> menuItems) {
        this.name = name;
        this.menuItems = menuItems;
        menuItems.forEach(menuItem -> menuItem.setRestaurant(this));
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<MenuItem> getMenuItems() {
        return menuItems;
    }
}
```

- [ ] **Step 5: Create the `RestaurantRepository`**

Delete `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/.gitkeep` and create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepository.java`:

```java
package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestaurantRepository extends JpaRepository<Restaurant, Long> {
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :ftgo-restaurant-service:test --tests "com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepositoryTest"`
Expected: PASS (1 test)

- [ ] **Step 7: Add the data seeder**

Create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/DataSeeder.java`:

```java
package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
public class DataSeeder implements CommandLineRunner {

    private final RestaurantRepository restaurantRepository;

    public DataSeeder(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @Override
    public void run(String... args) {
        if (restaurantRepository.count() > 0) {
            return;
        }
        restaurantRepository.save(new Restaurant("Ajanta Indian Cuisine", List.of(
                new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99")),
                new MenuItem("Garlic Naan", new BigDecimal("3.50"))
        )));
        restaurantRepository.save(new Restaurant("Pizza Palace", List.of(
                new MenuItem("Margherita Pizza", new BigDecimal("12.00")),
                new MenuItem("Pepperoni Pizza", new BigDecimal("13.50"))
        )));
    }
}
```

`DataSeeder` is a `@Component`, so it is not picked up by `@DataJpaTest` (which only scans JPA-related beans) — the repository test from Step 1 is unaffected.

- [ ] **Step 8: Run the full restaurant-service test suite to confirm nothing broke**

Run: `./gradlew :ftgo-restaurant-service:test`
Expected: PASS (`RestaurantRepositoryTest` and `FtgoRestaurantServiceApplicationTests` both green)

- [ ] **Step 9: Commit**

```bash
git add ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/Restaurant.java \
        ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/MenuItem.java \
        ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepository.java \
        ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/DataSeeder.java \
        ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/infrastructure/RestaurantRepositoryTest.java
git rm ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/domain/.gitkeep \
       ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/infrastructure/.gitkeep
git commit -m "feat(restaurant-service): add Restaurant/MenuItem domain model, repository, seed data"
```

---

### Task 2: restaurant-service — REST API endpoint

**Files:**
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantResponse.java`
- Create: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java`
- Test: `ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/api/RestaurantControllerTest.java`
- Delete: `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/.gitkeep`

**Interfaces:**
- Consumes: `Restaurant.getId()/getName()/getMenuItems()`, `MenuItem.getId()/getName()/getPrice()` (Task 1), `RestaurantRepository.findById(Long): Optional<Restaurant>` (Task 1).
- Produces: `GET /restaurants/{id}` → `200` with body `{id, name, menuItems:[{id,name,price}]}`, or `404` if not found. This is the endpoint `order-service`'s `RestaurantServiceProxy` (Task 4) will call.

- [ ] **Step 1: Write the failing controller test**

Create `ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/api/RestaurantControllerTest.java`:

```java
package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RestaurantController.class)
class RestaurantControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RestaurantRepository restaurantRepository;

    @Test
    void returnsRestaurantWhenFound() throws Exception {
        MenuItem menuItem = new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99"));
        setId(menuItem, 10L);
        Restaurant restaurant = new Restaurant("Ajanta Indian Cuisine", List.of(menuItem));
        setId(restaurant, 1L);

        when(restaurantRepository.findById(1L)).thenReturn(Optional.of(restaurant));

        mockMvc.perform(get("/restaurants/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("Ajanta Indian Cuisine"))
                .andExpect(jsonPath("$.menuItems[0].id").value(10))
                .andExpect(jsonPath("$.menuItems[0].name").value("Chicken Tikka Masala"))
                .andExpect(jsonPath("$.menuItems[0].price").value(14.99));
    }

    @Test
    void returns404WhenNotFound() throws Exception {
        when(restaurantRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/restaurants/99"))
                .andExpect(status().isNotFound());
    }

    private static void setId(Object entity, Long id) throws Exception {
        Field idField = entity.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
    }
}
```

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `./gradlew :ftgo-restaurant-service:test --tests "com.sanjay.ftgo.restaurant.api.RestaurantControllerTest"`
Expected: FAIL — `RestaurantController`/`RestaurantResponse` cannot be resolved.

- [ ] **Step 3: Create the `RestaurantResponse` DTO**

Delete `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/.gitkeep` and create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantResponse.java`:

```java
package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.Restaurant;

import java.math.BigDecimal;
import java.util.List;

public record RestaurantResponse(Long id, String name, List<MenuItemResponse> menuItems) {

    public record MenuItemResponse(Long id, String name, BigDecimal price) {
    }

    public static RestaurantResponse from(Restaurant restaurant) {
        List<MenuItemResponse> items = restaurant.getMenuItems().stream()
                .map(menuItem -> new MenuItemResponse(menuItem.getId(), menuItem.getName(), menuItem.getPrice()))
                .toList();
        return new RestaurantResponse(restaurant.getId(), restaurant.getName(), items);
    }
}
```

- [ ] **Step 4: Create the `RestaurantController`**

Create `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java`:

```java
package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/restaurants")
public class RestaurantController {

    private final RestaurantRepository restaurantRepository;

    public RestaurantController(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @GetMapping("/{id}")
    public ResponseEntity<RestaurantResponse> getRestaurant(@PathVariable Long id) {
        return restaurantRepository.findById(id)
                .map(RestaurantResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ftgo-restaurant-service:test --tests "com.sanjay.ftgo.restaurant.api.RestaurantControllerTest"`
Expected: PASS (2 tests)

- [ ] **Step 6: Run the full restaurant-service test suite**

Run: `./gradlew :ftgo-restaurant-service:test`
Expected: PASS (all tests green)

- [ ] **Step 7: Commit**

```bash
git add ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantResponse.java \
        ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java \
        ftgo-restaurant-service/src/test/java/com/sanjay/ftgo/restaurant/api/RestaurantControllerTest.java
git rm ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/.gitkeep
git commit -m "feat(restaurant-service): add GET /restaurants/{id} endpoint"
```

---

### Task 3: order-service — domain model, port, exceptions, OrderService

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantInfo.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServicePort.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantNotFoundException.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServiceUnavailableException.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/MenuItemNotFoundException.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/.gitkeep`

**Interfaces:**
- Produces: `RestaurantServicePort { RestaurantInfo findRestaurant(Long restaurantId) }` — implemented by `RestaurantServiceProxy` in Task 4. `RestaurantInfo(Long id, String name, List<MenuItemInfo> menuItems)` with nested `MenuItemInfo(Long id, String name, BigDecimal price)` — this is the JSON shape `RestaurantServiceProxy` deserializes from restaurant-service's `RestaurantResponse` (Task 2), so field names must match exactly. `OrderService.createOrder(Long restaurantId, List<OrderLineItem> lineItems): Order` — used by `OrderController` in Task 5. `OrderLineItem(Long menuItemId, int quantity)`. `Order.getId()/getRestaurantId()/getLineItems()/getStatus()`. Exceptions `RestaurantNotFoundException`, `RestaurantServiceUnavailableException`, `MenuItemNotFoundException` — all `RuntimeException` subclasses, thrown by `OrderService.createOrder` and handled by `OrderController` in Task 5.

- [ ] **Step 1: Write the failing `OrderService` test using a fake port**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderServiceTest {

    private final RestaurantInfo restaurant = new RestaurantInfo(1L, "Ajanta Indian Cuisine", List.of(
            new RestaurantInfo.MenuItemInfo(10L, "Chicken Tikka Masala", new BigDecimal("14.99")),
            new RestaurantInfo.MenuItemInfo(11L, "Garlic Naan", new BigDecimal("3.50"))
    ));

    private final RestaurantServicePort fakePort = restaurantId ->
            restaurantId.equals(1L) ? restaurant : null;

    private final OrderService orderService = new OrderService(fakePort);

    @Test
    void createsOrderWhenRestaurantAndMenuItemsAreValid() {
        Order order = orderService.createOrder(1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: FAIL — domain classes not yet defined.

- [ ] **Step 3: Create `OrderStatus`, `OrderLineItem`, `Order`**

Delete `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/.gitkeep`.

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderStatus.java`:

```java
package com.sanjay.ftgo.order.domain;

public enum OrderStatus {
    APPROVED,
    REJECTED
}
```

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderLineItem.java`:

```java
package com.sanjay.ftgo.order.domain;

public record OrderLineItem(Long menuItemId, int quantity) {
}
```

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/Order.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.util.List;

public class Order {

    private final Long id;
    private final Long restaurantId;
    private final List<OrderLineItem> lineItems;
    private final OrderStatus status;

    public Order(Long id, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this.id = id;
        this.restaurantId = restaurantId;
        this.lineItems = lineItems;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public List<OrderLineItem> getLineItems() {
        return lineItems;
    }

    public OrderStatus getStatus() {
        return status;
    }
}
```

- [ ] **Step 4: Create `RestaurantInfo` and `RestaurantServicePort`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantInfo.java`:

```java
package com.sanjay.ftgo.order.domain;

import java.math.BigDecimal;
import java.util.List;

public record RestaurantInfo(Long id, String name, List<MenuItemInfo> menuItems) {

    public record MenuItemInfo(Long id, String name, BigDecimal price) {
    }
}
```

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServicePort.java`:

```java
package com.sanjay.ftgo.order.domain;

public interface RestaurantServicePort {

    RestaurantInfo findRestaurant(Long restaurantId);
}
```

- [ ] **Step 5: Create the exception types**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantNotFoundException.java`:

```java
package com.sanjay.ftgo.order.domain;

public class RestaurantNotFoundException extends RuntimeException {

    public RestaurantNotFoundException(Long restaurantId) {
        super("Restaurant not found: " + restaurantId);
    }
}
```

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/RestaurantServiceUnavailableException.java`:

```java
package com.sanjay.ftgo.order.domain;

public class RestaurantServiceUnavailableException extends RuntimeException {

    public RestaurantServiceUnavailableException(Long restaurantId, Throwable cause) {
        super("Restaurant service unavailable while looking up restaurant: " + restaurantId, cause);
    }
}
```

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/MenuItemNotFoundException.java`:

```java
package com.sanjay.ftgo.order.domain;

public class MenuItemNotFoundException extends RuntimeException {

    public MenuItemNotFoundException(Long menuItemId, Long restaurantId) {
        super("Menu item " + menuItemId + " not found for restaurant " + restaurantId);
    }
}
```

- [ ] **Step 6: Create `OrderService`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/OrderService.java`:

```java
package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final AtomicLong idGenerator = new AtomicLong(1);

    public OrderService(RestaurantServicePort restaurantServicePort) {
        this.restaurantServicePort = restaurantServicePort;
    }

    public Order createOrder(Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        return new Order(idGenerator.getAndIncrement(), restaurantId, lineItems, OrderStatus.APPROVED);
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.domain.OrderServiceTest"`
Expected: PASS (2 tests)

- [ ] **Step 8: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all tests green)

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/
git rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/domain/.gitkeep 2>/dev/null || true
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/domain/OrderServiceTest.java
git commit -m "feat(order-service): add Order domain model, RestaurantServicePort, and OrderService"
```

---

### Task 4: order-service — RestaurantServiceProxy (RestClient + circuit breaker)

**Files:**
- Modify: `ftgo-order-service/build.gradle`
- Modify: `ftgo-order-service/src/main/resources/application.yml`
- Modify: `ftgo-order-service/src/test/resources/application.yml`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxyTest.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/.gitkeep`

**Interfaces:**
- Consumes: `RestaurantServicePort` (Task 3, implemented here), `RestaurantInfo`/`RestaurantInfo.MenuItemInfo` (Task 3, deserialization target), `RestaurantNotFoundException`, `RestaurantServiceUnavailableException` (Task 3).
- Produces: `RestaurantServiceProxy implements RestaurantServicePort` — a `@Component` that `OrderService` (Task 3, autowired by Spring in Task 5) receives via constructor injection.

- [ ] **Step 1: Add Resilience4j, AOP, and WireMock dependencies**

Edit `ftgo-order-service/build.gradle`, replacing the placeholder comment:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-aop'
    implementation 'io.github.resilience4j:resilience4j-spring-boot3:2.2.0'

    testImplementation 'org.wiremock:wiremock-standalone:3.9.2'
}
```

- [ ] **Step 2: Add restaurant-service base URL and circuit breaker config**

Edit `ftgo-order-service/src/main/resources/application.yml`, appending:

```yaml
restaurant-service:
  base-url: http://localhost:8085

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 5s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

`ignore-exceptions` keeps ordinary "restaurant not found" 404s from counting as circuit-breaker failures — only real connectivity/timeout problems should trip the breaker.

- [ ] **Step 3: Add a WireMock-backed test base URL to the test config**

Edit `ftgo-order-service/src/test/resources/application.yml`, appending:

```yaml
restaurant-service:
  base-url: http://localhost:8089

resilience4j:
  circuitbreaker:
    instances:
      restaurantService:
        sliding-window-size: 4
        failure-rate-threshold: 50
        wait-duration-in-open-state: 2s
        permitted-number-of-calls-in-half-open-state: 2
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.sanjay.ftgo.order.domain.RestaurantNotFoundException
```

- [ ] **Step 4: Write the failing `RestaurantServiceProxy` test, including a forced circuit-open scenario**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxyTest.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
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
@DirtiesContext
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
}
```

The third test drives the 4-call sliding window (configured in Step 3) past the 50% failure threshold with real connection failures, confirming the circuit opens and short-circuits even after the server comes back up (until the 2s wait duration elapses).

- [ ] **Step 5: Run test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.RestaurantServiceProxyTest"`
Expected: FAIL — `RestaurantServiceProxy` not yet defined.

- [ ] **Step 6: Create `RestClientConfig`**

Delete `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/.gitkeep`.

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestClientConfig.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient restaurantServiceRestClient(@Value("${restaurant-service.base-url}") String baseUrl) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(2));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect().build(settings);

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }
}
```

- [ ] **Step 7: Create `RestaurantServiceProxy`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxy.java`:

```java
package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class RestaurantServiceProxy implements RestaurantServicePort {

    private final RestClient restClient;

    public RestaurantServiceProxy(RestClient restaurantServiceRestClient) {
        this.restClient = restaurantServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "restaurantService", fallbackMethod = "findRestaurantFallback")
    public RestaurantInfo findRestaurant(Long restaurantId) {
        try {
            return restClient.get()
                    .uri("/restaurants/{id}", restaurantId)
                    .retrieve()
                    .body(RestaurantInfo.class);
        } catch (HttpClientErrorException.NotFound e) {
            throw new RestaurantNotFoundException(restaurantId);
        }
    }

    @SuppressWarnings("unused")
    private RestaurantInfo findRestaurantFallback(Long restaurantId, Throwable throwable) {
        if (throwable instanceof RestaurantNotFoundException notFound) {
            throw notFound;
        }
        throw new RestaurantServiceUnavailableException(restaurantId, throwable);
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.infrastructure.RestaurantServiceProxyTest"`
Expected: PASS (3 tests)

- [ ] **Step 9: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all tests green)

- [ ] **Step 10: Commit**

```bash
git add ftgo-order-service/build.gradle \
        ftgo-order-service/src/main/resources/application.yml \
        ftgo-order-service/src/test/resources/application.yml \
        ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/
git rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/infrastructure/.gitkeep 2>/dev/null || true
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/infrastructure/RestaurantServiceProxyTest.java
git commit -m "feat(order-service): add RestaurantServiceProxy with RestClient and Resilience4j circuit breaker"
```

---

### Task 5: order-service — OrderController and exception handling

**Files:**
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/CreateOrderRequest.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderResponse.java`
- Create: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`
- Test: `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`
- Delete: `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/.gitkeep`

**Interfaces:**
- Consumes: `OrderService.createOrder(Long, List<OrderLineItem>): Order` (Task 3), `Order.getId()/getRestaurantId()/getLineItems()/getStatus()` (Task 3), `OrderLineItem(Long, int)` (Task 3), `RestaurantNotFoundException`, `MenuItemNotFoundException`, `RestaurantServiceUnavailableException` (Task 3).
- Produces: `POST /orders` — request body `{restaurantId, lineItems:[{menuItemId, quantity}]}`, response `201` with `{id, restaurantId, lineItems:[{menuItemId,quantity}], status}` on success; `404` on `RestaurantNotFoundException`/`MenuItemNotFoundException`; `503` on `RestaurantServiceUnavailableException`.

- [ ] **Step 1: Write the failing controller test**

Create `ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderStatus;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @Test
    void createsOrderSuccessfully() throws Exception {
        Order order = new Order(1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVED);
        when(orderService.createOrder(eq(1L), any())).thenReturn(order);

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":2}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.restaurantId").value(1))
                .andExpect(jsonPath("$.status").value("APPROVED"));
    }

    @Test
    void returns404WhenRestaurantNotFound() throws Exception {
        when(orderService.createOrder(eq(99L), any())).thenThrow(new RestaurantNotFoundException(99L));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":99,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns404WhenMenuItemNotFound() throws Exception {
        when(orderService.createOrder(eq(1L), any())).thenThrow(new MenuItemNotFoundException(999L, 1L));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":999,"quantity":1}]}
                                """))
                .andExpect(status().isNotFound());
    }

    @Test
    void returns503WhenRestaurantServiceUnavailable() throws Exception {
        when(orderService.createOrder(eq(1L), any()))
                .thenThrow(new RestaurantServiceUnavailableException(1L, new RuntimeException("timeout")));

        mockMvc.perform(post("/orders")
                        .contentType("application/json")
                        .content("""
                                {"restaurantId":1,"lineItems":[{"menuItemId":10,"quantity":1}]}
                                """))
                .andExpect(status().isServiceUnavailable());
    }
}
```

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: FAIL — `OrderController`/`CreateOrderRequest`/`OrderResponse` not yet defined.

- [ ] **Step 3: Create `CreateOrderRequest`**

Delete `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/.gitkeep` and create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/CreateOrderRequest.java`:

```java
package com.sanjay.ftgo.order.api;

import java.util.List;

public record CreateOrderRequest(Long restaurantId, List<LineItemRequest> lineItems) {

    public record LineItemRequest(Long menuItemId, int quantity) {
    }
}
```

- [ ] **Step 4: Create `OrderResponse`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderResponse.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.Order;

import java.util.List;

public record OrderResponse(Long id, Long restaurantId, List<LineItemResponse> lineItems, String status) {

    public record LineItemResponse(Long menuItemId, int quantity) {
    }

    public static OrderResponse from(Order order) {
        List<LineItemResponse> items = order.getLineItems().stream()
                .map(lineItem -> new LineItemResponse(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderResponse(order.getId(), order.getRestaurantId(), items, order.getStatus().name());
    }
}
```

- [ ] **Step 5: Create `OrderController`**

Create `ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/OrderController.java`:

```java
package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        List<OrderLineItem> lineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();

        Order order = orderService.createOrder(request.restaurantId(), lineItems);
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(order));
    }

    @ExceptionHandler({RestaurantNotFoundException.class, MenuItemNotFoundException.class})
    public ResponseEntity<String> handleNotFound(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(RestaurantServiceUnavailableException.class)
    public ResponseEntity<String> handleUnavailable(RestaurantServiceUnavailableException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
    }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :ftgo-order-service:test --tests "com.sanjay.ftgo.order.api.OrderControllerTest"`
Expected: PASS (4 tests)

- [ ] **Step 7: Run the full order-service test suite**

Run: `./gradlew :ftgo-order-service:test`
Expected: PASS (all tests green)

- [ ] **Step 8: Run both services' full test suites together**

Run: `./gradlew test`
Expected: PASS (all modules green)

- [ ] **Step 9: Commit**

```bash
git add ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/
git rm ftgo-order-service/src/main/java/com/sanjay/ftgo/order/api/.gitkeep 2>/dev/null || true
git add ftgo-order-service/src/test/java/com/sanjay/ftgo/order/api/OrderControllerTest.java
git commit -m "feat(order-service): add POST /orders endpoint with 404/503 error handling"
```

---

### Task 6: Manual end-to-end verification and CONTEXT.md update

**Files:**
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: Everything from Tasks 1-5 (both services fully wired).
- Produces: Updated progress tracker; no new code.

- [ ] **Step 1: Start infrastructure**

Run: `docker compose up -d mysql`
Expected: `mysql` container becomes healthy (check with `docker compose ps`).

- [ ] **Step 2: Start restaurant-service in one terminal**

Run: `./gradlew :ftgo-restaurant-service:bootRun`
Expected: Log line `Started FtgoRestaurantServiceApplication` on port 8085, with seed data inserted (2 restaurants).

- [ ] **Step 3: Start order-service in a second terminal**

Run: `./gradlew :ftgo-order-service:bootRun`
Expected: Log line `Started FtgoOrderServiceApplication` on port 8082.

- [ ] **Step 4: Verify the happy path**

Run: `curl -s -X POST http://localhost:8082/orders -H "Content-Type: application/json" -d '{"restaurantId":1,"lineItems":[{"menuItemId":1,"quantity":2}]}'`
Expected: `201`-shaped JSON body with `"status":"APPROVED"`. (Restaurant/menu item IDs are auto-generated by MySQL `IDENTITY` — check the actual IDs via `curl http://localhost:8085/restaurants/1` first if `1`/`1` don't resolve, since this is the first-ever insert into a fresh schema they should be `1`.)

- [ ] **Step 5: Verify the circuit breaker trips when restaurant-service is down**

Stop restaurant-service (Ctrl+C in its terminal), then run the same `curl -X POST http://localhost:8082/orders ...` command 4-5 times in a row.
Expected: First one or two calls may take ~2s (RestClient read timeout) and return `503`; after the sliding window's failure threshold is crossed, subsequent calls fail fast (near-instant `503`) — this is the circuit breaker in the open state, not waiting for a fresh timeout each time.

- [ ] **Step 6: Verify recovery**

Restart restaurant-service, wait 5+ seconds (the configured `wait-duration-in-open-state`), then repeat the `curl -X POST` happy-path command.
Expected: `201` again — the circuit breaker has moved to half-open and then closed after a successful call.

- [ ] **Step 7: Update CONTEXT.md**

Edit `CONTEXT.md`:
- Change the Ch. 3 row status in the "Book structure & progress" table from `Not started` to `Implementing`.
- Update "Current position" section: status to `Implementing — RPI + circuit breaker done`, last session date to today.
- Add a session log line noting the REST + circuit breaker pattern was implemented between order-service and restaurant-service.
- In "Patterns reference" → "Communication", check off `Remote procedure invocation / REST (Ch. 3)` and `Circuit breaker (Ch. 3)`.
- In "FTGO app build log" → "Services to build", update `ftgo-order-service` and `ftgo-restaurant-service` status from "Ready to scaffold" to reflect the new endpoints (e.g. "REST call + circuit breaker to restaurant-service" / "GET /restaurants/{id} implemented").

- [ ] **Step 8: Commit**

```bash
git add CONTEXT.md
git commit -m "docs: update CONTEXT.md — Ch.3 REST + circuit breaker pattern implemented"
```

---

## Self-Review Notes

- **Spec coverage:** minimal Restaurant/MenuItem domain + persistence (Task 1), `GET /restaurants/{id}` (Task 2), minimal in-memory Order + port (Task 3), RestClient + Resilience4j circuit breaker adapter (Task 4), `POST /orders` with 404/503/400 handling (Task 5), manual e2e + docs (Task 6). All spec sections are covered.
- **400 (malformed request):** not separately tested — Spring's default behavior for a request body that fails to deserialize (e.g. missing `Content-Type`, invalid JSON) already returns `400` via `HttpMessageNotReadableException` without any custom code; adding a handler for it would just be restating framework default behavior, so it's left as-is per YAGNI.
- **Type consistency:** verified `RestaurantInfo`/`RestaurantInfo.MenuItemInfo` field names (`id`, `name`, `menuItems`, `price`) match `RestaurantResponse`/`RestaurantResponse.MenuItemResponse` JSON output exactly, since `RestaurantServiceProxy` deserializes the latter into the former across service boundaries.
- **Placeholder scan:** no TBD/TODO markers; every step has complete code.
