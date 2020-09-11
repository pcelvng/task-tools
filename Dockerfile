
FROM alpine:3.10.2
ADD https://github.com/kelseyhightower/confd/releases/download/v0.16.0/confd-0.16.0-linux-amd64 /usr/bin/confd

RUN chmod +x /usr/bin/confd
RUN apk add ca-certificates
RUN apk add curl
RUN apk add jq

COPY build/ /usr/bin/