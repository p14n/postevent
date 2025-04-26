FROM eclipse-temurin:21.0.6_7-jre-alpine-3.21

# Install uberjar
ADD app-uber.jar /opt/postevent/app-uber.jar

# Run marketing-service.sh start script by default
ENTRYPOINT ["java","-jar","/opt/postevent/app-uber.jar"]