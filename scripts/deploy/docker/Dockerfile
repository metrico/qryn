FROM golang:1.24.0-alpine as builder

COPY . /src

WORKDIR /src

ARG VIEW

RUN if [ "$VIEW" = "1" ]; then \
        go build -tags view -o gigapipe . ; \
    else \
        go build -o gigapipe . ; \
    fi

FROM alpine:3.21

COPY --from=builder /src/gigapipe /gigapipe

CMD /gigapipe