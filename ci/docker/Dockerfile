ARG BASE_VERSION

FROM adoptopenjdk/openjdk11:jdk-11.0.4_11
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD gradle.properties ./gradle.properties
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean pravegaClientJars; \
    ./gradlew jar

FROM emcmongoose/mongoose-base:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_PREEMPT_VERSION
ARG VERSION
RUN mkdir -p $HOME/.mongoose/${BASE_VERSION}/ext
COPY --from=0 /build/libs/mongoose-storage-driver-pravega-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_pravega.sh /opt/mongoose/entrypoint_storage_driver_pravega.sh
RUN mv -f /tmp/mongoose-storage-driver-pravega-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/ \
    && chmod +x /opt/mongoose/entrypoint_storage_driver_pravega.sh \
    && curl https://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-preempt/${STORAGE_DRIVER_PREEMPT_VERSION}/mongoose-storage-driver-preempt-${STORAGE_DRIVER_PREEMPT_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-preempt.jar
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_pravega.sh"]
