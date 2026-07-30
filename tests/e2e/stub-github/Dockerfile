FROM golang:1.25.12-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init stub-github && CGO_ENABLED=0 go build -o /out/stub-github .

FROM alpine:3.20
COPY --from=build /out/stub-github /usr/local/bin/stub-github
EXPOSE 9200
USER 65534:65534
ENTRYPOINT ["stub-github"]
