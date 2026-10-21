# Runtime image for one RaftKV node. Build the jar first:  ./mvnw -B package -DskipTests
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY raftkv-core/target/raftkv.jar /app/raftkv.jar
VOLUME /data
EXPOSE 7000
ENTRYPOINT ["java", "--sun-misc-unsafe-memory-access=allow", "-Xmx256m", "-jar", "/app/raftkv.jar"]
