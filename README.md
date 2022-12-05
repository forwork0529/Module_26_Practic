Во время решения задания столкнулся с проблемой - при помещении программы будь то моей , будь то эталонной ( из примера 20 модуля) в контейнер в stdin слал сообщения какой то неведомый мне процесс..
Обсуждение в Slack: https://app.slack.com/client/T01A2B3MSA2/C01QAJG3M8U/thread/C01QAJG3M8U-1670144562.692479
Пришёл к следующему решению:

1. Заменил ENTRYPOINT 

FROM golang
RUN mkdir -p /go/src/NewPipeLine
WORKDIR /go/src/NewPipeLine
ADD /. .
RUN go install .

FROM blang/alpine-bash
LABEL version="1.0.0"
LABEL maintainer="Ilya<test@test.ru>"
WORKDIR /root/
COPY --from=0 /go/bin/NewPipeLine .
ENTRYPOINT /bin/sh
EXPOSE 8080

2. Выбрал другие опции для взаимодействия с приложением в запущенном контейнере

docker run -ti -d --name NewPipeLine333 newpipeline333 /bin/bash
docker exec -ti NewPipeLine333 /bin/bash
В контейнере: /root/NewPipeLine

