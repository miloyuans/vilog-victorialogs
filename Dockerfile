FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/vilog-victorialogs ./cmd/vilog-victorialogs

FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/vilog-victorialogs /usr/local/bin/vilog-victorialogs
COPY config.example.yaml /app/config.yaml

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/vilog-victorialogs", "-config", "/app/config.yaml"]
