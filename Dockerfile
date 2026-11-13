# Runtime image for the Building IoT app. Build the jar first:  mvn -DskipTests package
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/building-iot.jar /app/building-iot.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=60", "-jar", "/app/building-iot.jar"]
