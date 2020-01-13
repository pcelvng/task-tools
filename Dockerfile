# public docker image
# docker build -t jeremiahz/task-tools:v0.6.0 .

FROM golang:1.13 as task
COPY ./ $GOPATH/src/github.com/pcelvng/task-tools
WORKDIR $GOPATH/src/github.com/pcelvng/task-tools
RUN CGO_ENABLED=0 GOOS=linux make

# main image with just the binaries / much smaller image
# includes the confd binary version v0.16.0 downloaded from github
FROM alpine:3.10.2
ADD https://github.com/kelseyhightower/confd/releases/download/v0.16.0/confd-0.16.0-linux-amd64 /usr/bin/confd

RUN chmod +x /usr/bin/confd
RUN apk add ca-certificates
RUN apk add curl

COPY --from=task /go/src/github.com/pcelvng/task-tools/build/ /usr/bin/