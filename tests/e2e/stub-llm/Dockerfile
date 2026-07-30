FROM golang:1.25.12-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init stub-llm && CGO_ENABLED=0 go build -o /out/stub-llm .

FROM alpine:3.20
COPY --from=build /out/stub-llm /usr/local/bin/stub-llm
EXPOSE 9100
USER 65534:65534
ENTRYPOINT ["stub-llm"]
