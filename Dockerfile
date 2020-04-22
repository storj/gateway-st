ARG DOCKER_ARCH
FROM ${DOCKER_ARCH:-amd64}/alpine
ARG TAG
ARG GOARCH
ENV GOARCH ${GOARCH}
EXPOSE 7777
WORKDIR /app
VOLUME /root/.local/share/storj/gateway
COPY release/${TAG}/gateway_linux_${GOARCH:-amd64} /app/gateway
COPY entrypoint /entrypoint
ENTRYPOINT ["/entrypoint"]
ENV STORJ_CONFIG_DIR=/root/.local/share/storj/gateway
ENV STORJ_SERVER_ADDRESS=0.0.0.0:7777
