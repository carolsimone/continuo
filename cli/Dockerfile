# syntax=docker/dockerfile:1.6
FROM golang:1.26.7-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /out/continuo ./cmd/continuo

FROM alpine:3.24
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/continuo /usr/local/bin/continuo
ENTRYPOINT ["continuo"]
