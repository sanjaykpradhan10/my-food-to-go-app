FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :ftgo-config-server:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/ftgo-config-server/build/libs/*.jar app.jar
# The git backend (see application.yml) needs an actual git working copy to read config-repo/
# from — a plain "COPY config-repo/" would not be a git repository, and Spring Cloud Config
# Server's git backend requires one even for local/file:// URIs. Copying the whole build-stage
# workspace (which is a checkout of this repo, .git included) is the simplest way to give the
# runtime image something JGit can open.
COPY --from=build /workspace /config-source-repo
EXPOSE 8888
ENTRYPOINT ["java", "-jar", "app.jar"]
