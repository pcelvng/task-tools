# to build this image you must specify the Dockerfile independently of the build context, using -f.
# docker build -f apps/utils/nsq-monitor/Dockerfile .
# this must be built from the project directory

FROM repo.rmgops.com/docker-remote/hydronica/task-tools:v0.28.0 as task-tools

FROM alpine:3.20

RUN apk add ca-certificates
RUN apk add curl

COPY --from=task-tools /usr/local/bin/confd /usr/local/bin/confd
COPY --from=task-tools /usr/local/bin/ll /usr/local/bin/ll

RUN mkdir /utils
COPY deploy/bin/nsq-monitor /usr/local/bin/nsq-monitor
COPY deploy/confd/conf.d/nsq-monitor.toml /etc/confd/conf.d/nsq-monitor.toml
COPY deploy/confd/templates/nsq-monitor.tmpl /etc/confd/templates/nsq-monitor.tmpl

CMD [ "sh","-c","confd -onetime -backend vault -node $VAULT_ADDR \
  -prefix $VAULT_PATH -scheme https -auth-type app-role \
  -role-id $ROLE_ID -secret-id $SECRET_ID; \
  nsq-monitor -config /utils/nsq-monitor.toml" ]
