FROM golang:1.23.4-alpine as builder
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
ENV PORT 3100
EXPOSE 3100
CMD /gigapipe
