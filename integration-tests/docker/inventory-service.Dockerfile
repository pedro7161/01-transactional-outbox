FROM eclipse-temurin:21-jre
WORKDIR /app
COPY inventory-service/target/quarkus-app/ ./
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
