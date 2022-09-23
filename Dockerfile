# syntax=docker/dockerfile:experimental

FROM golang:1.19-bullseye AS builder

RUN apt-get update && \
    apt-get install git

WORKDIR /src/zero-pod-autoscaler
COPY go.mod go.sum /src/zero-pod-autoscaler/
RUN go mod download

COPY . ./

ENV GODEBUG=netdns=go+1
RUN CGO_ENABLED=0 go build .

#############################
FROM debian:bullseye
ENV GODEBUG=netdns=go+1
COPY --from=builder /src/zero-pod-autoscaler/zero-pod-autoscaler /bin/zero-pod-autoscaler
ENTRYPOINT [ "/bin/zero-pod-autoscaler" ]
