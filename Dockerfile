FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ai-gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/ai-gateway /app/ai-gateway
COPY configs/gateway.example.json /app/configs/gateway.json
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/ai-gateway", "-config", "/app/configs/gateway.json"]
