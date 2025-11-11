FROM maven:3.9.11-eclipse-temurin-17
WORKDIR app/
COPY . . 

RUN --mount=type=cache,target=/root/.m2 \
    --mount=type=cache,target=/root/.sbt \
    mvn compile

RUN --mount=type=cache,target=/root/.m2 \
    --mount=type=cache,target=/root/.sbt \
    mvn package

RUN bash -c "mv target/*dependencies.jar odibel.jar"

ENTRYPOINT ["java", "-cp", "odibel.jar", "ai.scads.odibel.main.Main"]
