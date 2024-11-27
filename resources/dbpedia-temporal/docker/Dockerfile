# Step 1: Base Image auswählen
FROM openjdk:8

# Step 2: Arbeitsverzeichnis festlegen
WORKDIR /app

# Step 3: Abhängigkeiten installieren
RUN apt-get update && apt-get install -y git maven

# Step 4: Code klonen
#RUN git clone https://github.com/Maximiliantoepfer/extraction-framework.git

# Step 5: Arbeitsverzeichnis ändern
WORKDIR /app/extraction-framework
RUN mkdir -p /app/extraction-framework/server/src/main/statistics

# Step 6: Projekt bauen
Add target/dief/server/target/*.jar server/
Add taget/dief/server/server.default.properties server/
Add target/dief/server/src/main/statistics server/src/main/statistics/
Add target/dief/mappings mappings/
Add target/dief/ontology.xml .
Add target/dief/ontology.owl .

WORKDIR /app/extraction-framework/server


#RUN mvn clean install -DskipTests -e -X

# Step 7: Shell-Skript ausführbar machen
#RUN chmod +x ./redeploy-server

# Step 8: Port 9999 exponieren
EXPOSE 9999

# Step 9: Shell-Skript ausführen
#CMD ["find", "../"]
CMD ["java", "-Xmx4096m", "-XX:-OmitStackTraceInFastThrow", "-XX:+PrintGC", "-XX:+PrintGCTimeStamps", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:+UseConcMarkSweepGC", "-cp", "server-4.2-SNAPSHOT-jar-with-dependencies.jar", "org.dbpedia.extraction.server.Server", "server.default.properties"]
