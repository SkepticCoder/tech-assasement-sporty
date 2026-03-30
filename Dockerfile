# ... (previous stages remain the same)

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=build /app/build/libs/betting-settlement-*.jar app.jar
COPY --from=build /opt/opentelemetry-javaagent.jar /opt/opentelemetry-javaagent.jar

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

ENV OTEL_SERVICE_NAME=betting-settlement
ENV OTEL_TRACES_EXPORTER=otlp
ENV OTEL_METRICS_EXPORTER=otlp
ENV OTEL_LOGS_EXPORTER=otlp
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
ENV OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

# JVM flags for crash diagnostics:
# -XX:+HeapDumpOnOutOfMemoryError – generate heap dump when OOM occurs
# -XX:HeapDumpPath – store heap dumps in the mounted volume
# -XX:ErrorFile – store JVM error logs (e.g., when native crash happens)
# -XX:+CreateCoredumpOnCrash – ensure a core dump is created for native crashes (may require ulimits)
# -Djava.io.tmpdir – set temporary directory to a location we can mount (optional)
ENV JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/coredumps -XX:ErrorFile=/tmp/coredumps/hs_err_pid%p.log -XX:+CreateCoredumpOnCrash -Djava.io.tmpdir=/tmp"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -javaagent:/opt/opentelemetry-javaagent.jar -Djava.security.egd=file:/dev/./urandom -jar app.jar"]