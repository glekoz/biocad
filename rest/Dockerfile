FROM golang:tip-alpine3.23 AS build
WORKDIR /src

COPY . .
RUN go build -o /usr/bin/biocad-web ./cmd/main.go

FROM ubuntu:24.04

COPY --from=build /usr/bin/biocad-web /usr/bin/biocad-web