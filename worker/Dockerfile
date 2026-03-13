FROM golang:1.24-alpine AS builder

WORKDIR /build

# Копируем файлы зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходный код
COPY . .

# Компилируем бинарник
RUN go build -o worker .

# Финальный образ
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Копируем скомпилированный бинарник из builder
COPY --from=builder /build/worker /app/worker

# Запускаем worker
CMD ["/app/worker"]