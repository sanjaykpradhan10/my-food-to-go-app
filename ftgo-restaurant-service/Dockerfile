FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-restaurant-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-restaurant-service/build/libs/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["sh", "-c", "[ -f /shared/dsn.env ] && export $(grep '^SENTRY_DSN=' /shared/dsn.env) ; exec java -jar app.jar"]
