# ---------- Stage 1: build the jar with Maven (JDK 21) ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy only the pom first and download dependencies. Docker caches this layer,
# so dependencies are re-downloaded only when pom.xml changes - not on every code edit.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
# Tests run in CI / locally; skipping them here keeps image builds fast.
RUN mvn -B -q package -DskipTests

# ---------- Stage 2: small runtime image (JRE only, no Maven, no sources) ----------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Don't run as root inside the container.
RUN addgroup -S spring && adduser -S spring -G spring
USER spring

COPY --from=build /app/target/order-producer-service-*.jar app.jar

EXPOSE 8081
# MaxRAMPercentage: size the heap from the container's memory limit.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
