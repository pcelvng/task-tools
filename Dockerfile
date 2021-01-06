
FROM alpine:3.12

RUN apk add ca-certificates
RUN apk add curl

#confd
ADD https://github.com/kelseyhightower/confd/releases/download/v0.16.0/confd-0.16.0-linux-amd64 /usr/bin/confd
RUN chmod +x /usr/bin/confd

#gojq
ADD https://github.com/itchyny/gojq/releases/download/v0.12.0/gojq_v0.12.0_linux_amd64.tar.gz /tmp/jq.tar.gz
RUN cd /tmp && tar -xzf jq.tar.gz
RUN cp /tmp/gojq_v0.12.0_linux_amd64/gojq /usr/bin/jq
RUN rm -rf /tmp/*

#ll
RUN echo -e "#!/bin/sh \n ls -Alhp \$1" > /usr/bin/ll
RUN chmod +x /usr/bin/ll

COPY build/ /usr/bin/