FROM eclipse-temurin:21-jre
WORKDIR /app
COPY order-service/target/quarkus-app/ ./
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
