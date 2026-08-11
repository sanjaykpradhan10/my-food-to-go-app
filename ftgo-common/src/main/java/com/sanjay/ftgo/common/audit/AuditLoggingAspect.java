package com.sanjay.ftgo.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.List;

// Applies to every @PostMapping + @PreAuthorize-gated @RestController method across all 9
// services, via ftgo-common's shared classpath - see AuditLoggingAutoConfiguration for how this
// bean gets registered without each service needing to declare it. Read-only (@GetMapping)
// endpoints are deliberately not matched: "who did what to which business object" is fully
// answered by mutating actions alone (Ch.11 §11.3.6 design doc), and auditing every read would
// add volume with no demonstrated need.
@Aspect
@Component
public class AuditLoggingAspect {

    private static final Logger log = LoggerFactory.getLogger(AuditLoggingAspect.class);

    private final KafkaTemplate<String, String> eventKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String serviceName;

    public AuditLoggingAspect(KafkaTemplate<String, String> eventKafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${spring.application.name}") String serviceName) {
        this.eventKafkaTemplate = eventKafkaTemplate;
        this.objectMapper = objectMapper;
        this.serviceName = serviceName;
    }

    @Around("@annotation(org.springframework.web.bind.annotation.PostMapping) "
            + "&& @annotation(org.springframework.security.access.prepost.PreAuthorize)")
    public Object auditMutatingCall(ProceedingJoinPoint joinPoint) throws Throwable {
        Jwt jwt = findJwtArgument(joinPoint);
        String action = describeAction(joinPoint);
        String entityType = describeEntityType(joinPoint);
        String entityId = describeEntityId(joinPoint);

        try {
            Object result = joinPoint.proceed();
            publish(jwt, action, entityType, entityId, AuditLogEntryEvent.SUCCESS, null);
            return result;
        } catch (Throwable t) {
            publish(jwt, action, entityType, entityId, AuditLogEntryEvent.FAILURE, t.getClass().getSimpleName());
            throw t;
        }
    }

    private Jwt findJwtArgument(ProceedingJoinPoint joinPoint) {
        for (Object arg : joinPoint.getArgs()) {
            if (arg instanceof Jwt jwt) {
                return jwt;
            }
        }
        return null;
    }

    private String describeAction(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        return "POST " + signature.getDeclaringType().getSimpleName() + "." + signature.getName();
    }

    private String describeEntityType(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String simpleName = signature.getDeclaringType().getSimpleName();
        return simpleName.endsWith("Controller")
                ? simpleName.substring(0, simpleName.length() - "Controller".length())
                : simpleName;
    }

    // Uses the first @PathVariable-annotated parameter, in declared order, as the business
    // object id - every mutating endpoint in this codebase that has one uses it as the sole
    // path variable identifying the object being acted on (e.g. OrderController.cancel(Long
    // id, ...)). Endpoints with none (e.g. createOrder, POST /consumers) still record the
    // actor/action with a null entityId; that's still useful for support/compliance even
    // without a pre-existing id.
    private String describeEntityId(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Parameter[] parameters = signature.getMethod().getParameters();
        Object[] args = joinPoint.getArgs();
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].isAnnotationPresent(org.springframework.web.bind.annotation.PathVariable.class)) {
                return args[i] == null ? null : String.valueOf(args[i]);
            }
        }
        return null;
    }

    private void publish(Jwt jwt, String action, String entityType, String entityId, String outcome,
                          String failureReason) {
        try {
            List<String> roles = jwt == null ? List.of() : jwt.getClaimAsStringList("roles");
            String userId = jwt == null ? null : jwt.getSubject();
            AuditLogEntryEvent event = new AuditLogEntryEvent(userId, roles == null ? List.of() : roles, action,
                    entityType, entityId, outcome, failureReason, serviceName, Instant.now());
            String payload = objectMapper.writeValueAsString(event);
            eventKafkaTemplate.send("audit-log", payload);
        } catch (Exception e) {
            // Best-effort: audit logging must never fail or block the request it's auditing.
            log.warn("Failed to publish audit log event for action {}", action, e);
        }
    }
}
