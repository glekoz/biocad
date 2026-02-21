FROM golang:1.25-alpine3.23 AS build
WORKDIR /src

COPY ./internal/repository/migrations ./migrations
COPY . .
RUN go install github.com/pressly/goose/v3/cmd/goose@v3.26.0 && \
    go build -o /usr/bin/biocad-worker ./cmd/main.go

FROM ubuntu:24.04

COPY --from=build /src/migrations /src/migrations
COPY --from=build /go/bin/goose /usr/local/bin/goose
COPY --from=build /usr/bin/biocad-worker /usr/bin/biocad-worker