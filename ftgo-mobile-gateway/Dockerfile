FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-mobile-gateway:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-mobile-gateway/build/libs/*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["sh", "-c", "[ -f /shared/dsn.env ] && export $(grep '^SENTRY_DSN=' /shared/dsn.env) ; exec java -jar app.jar"]
