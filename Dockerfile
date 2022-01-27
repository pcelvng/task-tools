
FROM alpine:3.15

RUN apk add ca-certificates
RUN apk add curl

#confd
ADD https://github.com/abtreece/confd/releases/download/v0.18.3/confd-v0.18.3-linux-amd64.tar.gz confd.tar.gz
RUN tar -xzf confd.tar.gz
RUN mv confd /usr/local/bin/confd
RUN chmod +x /usr/local/bin/confd

#gojq
ADD https://github.com/itchyny/gojq/releases/download/v0.12.0/gojq_v0.12.0_linux_amd64.tar.gz /tmp/jq.tar.gz
RUN cd /tmp && tar -xzf jq.tar.gz
RUN cp /tmp/gojq_v0.12.0_linux_amd64/gojq /usr/local/bin/jq
RUN rm -rf /tmp/*

#ll
RUN echo -e "#!/bin/sh \n ls -Alhp \$1" > /usr/local/bin/ll
RUN chmod +x /usr/local/bin/ll

COPY build/linux/ /usr/local/bin/
