# syntax=docker/dockerfile:experimental

FROM golang:1.19.1-bullseye AS builder

RUN apt-get update && \
    apt-get install git

WORKDIR /src/zero-pod-autoscaler
COPY go.mod go.sum /src/zero-pod-autoscaler/
RUN go mod download

COPY . ./

ENV GODEBUG=netdns=Cgo+2
RUN CGO_ENABLED=0 go build .

#############################
FROM debian:bullseye

COPY --from=builder /src/zero-pod-autoscaler/zero-pod-autoscaler /bin/zero-pod-autoscaler
ENTRYPOINT [ "GODEBUG=netdns=Cgo+2 /bin/zero-pod-autoscaler" ]
