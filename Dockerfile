FROM eclipse-temurin:21-jdk-alpine AS build

WORKDIR /app

COPY gradle/ gradle/
COPY gradlew build.gradle.kts settings.gradle.kts gradle/libs.versions.toml ./
RUN ./gradlew dependencies --no-daemon || true

COPY src/ src/
RUN ./gradlew bootJar --no-daemon

ARG OTEL_AGENT_VERSION=2.14.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /opt/opentelemetry-javaagent.jar

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=build /app/build/libs/bet-settlement-service-*.jar app.jar
COPY --from=build /opt/opentelemetry-javaagent.jar /opt/opentelemetry-javaagent.jar

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

ENV OTEL_SERVICE_NAME=bet-settlement-service
ENV OTEL_TRACES_EXPORTER=otlp
ENV OTEL_METRICS_EXPORTER=otlp
ENV OTEL_LOGS_EXPORTER=otlp
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
ENV OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

ENTRYPOINT ["java", "-javaagent:/opt/opentelemetry-javaagent.jar", "-Djava.security.egd=file:/dev/./urandom", "-jar", "app.jar"]
