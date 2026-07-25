package com.sanjay.ftgo.accounting.api;

import com.sanjay.ftgo.accounting.domain.Authorization;
import com.sanjay.ftgo.accounting.domain.AuthorizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuthorizationController.class)
class AuthorizationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AuthorizationRepository authorizationRepository;

    @Test
    void viewByOrderIdReturnsAuthorizationInfo() throws Exception {
        Authorization authorization = Authorization.authorize(42L, 3).authorization();
        when(authorizationRepository.findByOrderId(42L)).thenReturn(Optional.of(authorization));

        mockMvc.perform(get("/authorizations/order/42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(42))
                .andExpect(jsonPath("$.status").value("AUTHORIZED"));
    }

    @Test
    void viewByOrderIdReturns404WhenNoAuthorizationForOrder() throws Exception {
        when(authorizationRepository.findByOrderId(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/authorizations/order/99")).andExpect(status().isNotFound());
    }
}
