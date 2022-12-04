FROM golang
RUN mkdir -p /go/src/NewPipeLine
WORKDIR /go/src/NewPipeLine
ADD /. .
RUN go install .

FROM alpine:latest
LABEL version="1.0.0"
LABEL maintainer="Ilya<test@test.ru>"
WORKDIR /root/
COPY --from=0 /go/bin/NewPipeLine .
ENTRYPOINT ./NewPipeLine
EXPOSE 8080
