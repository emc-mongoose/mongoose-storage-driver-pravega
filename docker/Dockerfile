ARG MONGOOSE_VERSION

FROM emcmongoose/mongoose:${MONGOOSE_VERSION}

ARG MONGOOSE_VERSION

ADD build/libs/mongoose-storage-driver-pravega.jar /opt/mongoose/mongoose-storage-driver-pravega.jar
ADD docker/entrypoint-storage-driver-pravega.sh /opt/mongoose/entrypoint-storage-driver-pravega.sh
ADD docker/entrypoint-storage-driver-pravega-debug.sh /opt/mongoose/entrypoint-storage-driver-pravega-debug.sh

RUN mkdir $HOME/.mongoose/${MONGOOSE_VERSION}; \
    mkdir $HOME/.mongoose/${MONGOOSE_VERSION}/ext; \
    ln -s /opt/mongoose/mongoose-storage-driver-pravega.jar $HOME/.mongoose/${MONGOOSE_VERSION}/ext/mongoose-storage-driver-pravega.jar; \
    chmod +x /opt/mongoose/entrypoint-storage-driver-pravega.sh

ENTRYPOINT ["/opt/mongoose/entrypoint-storage-driver-pravega.sh"]
