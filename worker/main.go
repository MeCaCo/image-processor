package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disintegration/imaging"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/streadway/amqp"
)

type Task struct {
	TaskID     string      `json:"task_id"`
	Operations []Operation `json:"operations"`
	ObjectName string      `json:"object_name"`
	Bucket     string      `json:"bucket"`
}

type Operation struct {
	Type   string                 `json:"type"`
	Params map[string]interface{} `json:"params"`
}

type Worker struct {
	rabbitConn  *amqp.Connection
	rabbitCh    *amqp.Channel
	minioClient *minio.Client
	db          *sql.DB
}

func main() {
	log.Println("🚀 Starting Image Processing Worker...")

	// ОТЛАДКА: выводим все переменные окружения
	log.Println("Environment variables:")
	log.Printf("  DATABASE_URL: %s", os.Getenv("DATABASE_URL"))
	log.Printf("  RABBITMQ_URL: %s", os.Getenv("RABBITMQ_URL"))
	log.Printf("  MINIO_ENDPOINT: %s", os.Getenv("MINIO_ENDPOINT"))
	log.Printf("  MINIO_ACCESS_KEY: %s", os.Getenv("MINIO_ACCESS_KEY"))
	log.Printf("  MINIO_SECRET_KEY: %s", os.Getenv("MINIO_SECRET_KEY"))
	log.Printf("  MINIO_BUCKET: %s", os.Getenv("MINIO_BUCKET"))

	worker := &Worker{}

	// Подключение к RabbitMQ
	if err := worker.connectRabbitMQ(); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer worker.rabbitConn.Close()
	defer worker.rabbitCh.Close()

	// Подключение к MinIO
	if err := worker.connectMinIO(); err != nil {
		log.Fatalf("Failed to connect to MinIO: %v", err)
	}

	// Подключение к PostgreSQL
	if err := worker.connectDB(); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer worker.db.Close()

	// Объявляем очередь
	q, err := worker.rabbitCh.QueueDeclare(
		"image_tasks",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Получаем сообщения из очереди
	msgs, err := worker.rabbitCh.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	// Запускаем воркеры (5 параллельных обработчиков)
	for i := 0; i < 5; i++ {
		go worker.startWorker(i, msgs)
	}

	log.Println("✅ Worker started. Waiting for tasks...")

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("👋 Shutting down worker...")
}

func (w *Worker) startWorker(id int, msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		log.Printf("[Worker %d] Received task", id)

		var task Task
		if err := json.Unmarshal(msg.Body, &task); err != nil {
			log.Printf("[Worker %d] Failed to parse task: %v", id, err)
			msg.Nack(false, false)
			continue
		}

		w.updateTaskStatus(task.TaskID, "processing")

		if err := w.processTask(task); err != nil {
			log.Printf("[Worker %d] Failed to process task %s: %v", id, task.TaskID, err)
			w.updateTaskStatus(task.TaskID, "failed")
			msg.Nack(false, true)
			continue
		}

		msg.Ack(false)
		log.Printf("[Worker %d] Task %s completed", id, task.TaskID)
	}
}

func (w *Worker) processTask(task Task) error {
	log.Printf("[Worker] Processing task: %s", task.TaskID)

	// 1. Скачиваем из MinIO
	reader, err := w.minioClient.GetObject(
		context.Background(),
		task.Bucket,
		task.ObjectName,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to get object from MinIO: %v", err)
	}
	defer reader.Close()

	// 2. Декодируем изображение
	img, _, err := image.Decode(reader)
	if err != nil {
		return fmt.Errorf("failed to decode image: %v", err)
	}

	// 3. Применяем операции
	resultImg := img
	for _, op := range task.Operations {
		switch op.Type {
		case "resize":
			width := int(op.Params["width"].(float64))
			height := int(op.Params["height"].(float64))
			log.Printf("Resizing to %dx%d", width, height)
			resultImg = imaging.Resize(resultImg, width, height, imaging.Lanczos)
		default:
			log.Printf("Unknown operation type: %s", op.Type)
		}
	}

	// 4. Сохраняем в буфер
	buf := new(bytes.Buffer)
	err = imaging.Encode(buf, resultImg, imaging.JPEG)
	if err != nil {
		return fmt.Errorf("failed to encode image: %v", err)
	}

	// 5. Загружаем обратно в MinIO
	resultName := fmt.Sprintf("processed/%s.jpg", task.TaskID)
	_, err = w.minioClient.PutObject(
		context.Background(),
		task.Bucket,
		resultName,
		bytes.NewReader(buf.Bytes()),
		int64(buf.Len()),
		minio.PutObjectOptions{ContentType: "image/jpeg"},
	)
	if err != nil {
		return fmt.Errorf("failed to upload to MinIO: %v", err)
	}

	// ⭐ ВАЖНО: Сохраняем путь к результату в БД
	_, err = w.db.Exec(
		"UPDATE image_tasks SET result_path = $1, status = 'completed', updated_at = NOW() WHERE id = $2",
		resultName, task.TaskID,
	)
	if err != nil {
		log.Printf("Failed to update result path in DB: %v", err)
		// Не возвращаем ошибку, так как файл уже сохранён
	}

	log.Printf("[Worker] Task %s completed, saved to %s/%s", task.TaskID, task.Bucket, resultName)
	return nil
}

func (w *Worker) connectRabbitMQ() error {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		log.Printf("RABBITMQ_URL not set, using default")
		url = "amqp://guest:guest@rabbitmq:5672/"
	}
	log.Printf("Connecting to RabbitMQ at: %s", url)

	log.Printf("Waiting 5 seconds for RabbitMQ to fully initialize...")
	time.Sleep(5 * time.Second)

	var err error
	for i := 0; i < 30; i++ {
		w.rabbitConn, err = amqp.Dial(url)
		if err == nil {
			log.Printf("Successfully connected to RabbitMQ on attempt %d", i+1)
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/30): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ after 30 attempts: %v", err)
	}

	w.rabbitCh, err = w.rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %v", err)
	}
	return nil
}

func (w *Worker) connectMinIO() error {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}
	w.minioClient = client
	return nil
}

func (w *Worker) connectDB() error {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://app:secret@localhost:5432/image_processor?sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	w.db = db
	return nil
}

func (w *Worker) updateTaskStatus(taskID, status string) error {
	if w.db == nil {
		log.Printf("DB not connected, skipping status update for task %s: %s", taskID, status)
		return nil
	}
	_, err := w.db.Exec(
		"UPDATE image_tasks SET status = $1, updated_at = NOW() WHERE id = $2",
		status, taskID,
	)
	if err != nil {
		log.Printf("Failed to update task status: %v", err)
	}
	return err
}
