FROM openjdk:8-jre

LABEL authors="Lorenzo Daneo <l.daneo@theinnovationfactory.it> <l.daneo@betacom.it>"

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY target/redis-message-broker-1.0-SNAPSHOT.jar /usr/src/app/app.war

EXPOSE 8080

RUN ["/bin/sh", "-c", "echo using profile $PROFILE"]
CMD ["java", "-jar", "app.war"]