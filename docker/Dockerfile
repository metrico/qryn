# sudo docker build -t negbie/heplify-server:latest .

FROM golang:latest as builder

RUN go get -u -d -v github.com/negbie/heplify-server/
WORKDIR /go/src/github.com/negbie/heplify-server/cmd/heplify-server/
RUN set -x && go get -u -d -v .

RUN go get -u -v github.com/gobuffalo/packr/...
WORKDIR /go/bin/
RUN ./packr -i /go/src/github.com/negbie/heplify-server/database

WORKDIR /go/src/github.com/negbie/heplify-server/cmd/heplify-server/
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-s -w' -installsuffix cgo -o heplify-server .


FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/negbie/heplify-server/cmd/heplify-server/heplify-server .