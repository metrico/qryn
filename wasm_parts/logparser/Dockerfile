FROM golang:1.16-buster AS builder
WORKDIR /tmp/logparser
COPY logparser .
RUN go build -mod=readonly -o ./logparser ./cmd/

FROM scratch
COPY --from=builder /tmp/logparser/logparser /usr/bin/logparser
ENTRYPOINT ["logparser"]
