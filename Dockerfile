FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ ./cmd/api ./cmd/consumer ./cmd/seed

FROM alpine:3.20
RUN adduser -D -H -u 10001 app
WORKDIR /app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/api"]
