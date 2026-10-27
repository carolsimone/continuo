FROM golang:1.26-alpine AS build
WORKDIR /src
COPY echo.go .
RUN CGO_ENABLED=0 go build -o /echo echo.go

# alpine with no Python, like the runtime stage of the shipped Go service images.
FROM alpine:3.22
COPY --from=build /echo /echo
USER 65532:65532
ENTRYPOINT ["/echo"]
