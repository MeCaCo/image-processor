from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import RedirectResponse
import uuid
from datetime import datetime, timedelta
import os
import logging
import io
import json
import asyncio

from models import ImageTask
from database import SessionLocal
from minio import Minio
from minio.error import S3Error
import aio_pika

# Настройки MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "images")

# Создаем клиент MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

app = FastAPI(
    title="Image Processing Service",
    description="Async image processing with Go workers",
    version="0.1.0"
)

log = logging.getLogger(__name__)

# Убеждаемся, что бакет существует
try:
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
except S3Error as e:
    log.error(f"MinIO bucket error: {e}")


@app.get("/")
async def root():
    return {"message": "Image Processing API", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


async def send_to_rabbitmq(task_id: str, object_name: str, operations: list):
    try:
        connection = await aio_pika.connect_robust(
            os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        )

        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("image_tasks", durable=True)

            task_data = {
                "task_id": task_id,
                "operations": operations,
                "object_name": object_name,
                "bucket": MINIO_BUCKET
            }

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(task_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="image_tasks"
            )

            log.info(f"Task {task_id} sent to RabbitMQ")

    except Exception as e:
        log.error(f"Failed to send to RabbitMQ: {e}")


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id = str(uuid.uuid4())
        file_content = await file.read()
        file_size = len(file_content)

        # Сохраняем в MinIO
        object_name = f"originals/{file_id}_{file.filename}"
        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            io.BytesIO(file_content),
            file_size,
            content_type=file.content_type
        )

        # Сохраняем в БД
        db = SessionLocal()
        task = ImageTask(
            id=file_id,
            filename=file.filename,
            status="uploaded",
            original_path=object_name
        )
        db.add(task)
        db.commit()

        # Отправляем задачу в RabbitMQ
        operations = [{"type": "resize", "params": {"width": 300, "height": 300}}]
        asyncio.create_task(send_to_rabbitmq(file_id, object_name, operations))

        task.status = "processing"
        db.commit()
        db.close()

        return {
            "file_id": file_id,
            "filename": file.filename,
            "size": file_size,
            "status": "processing",
            "created_at": datetime.now().isoformat()
        }

    except Exception as e:
        log.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/files")
async def list_files():
    try:
        db = SessionLocal()
        tasks = db.query(ImageTask).all()
        db.close()

        return {
            "files": [
                {
                    "file_id": task.id,
                    "filename": task.filename,
                    "status": task.status,
                    "created_at": task.created_at
                }
                for task in tasks
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")


@app.get("/download/{task_id}")
async def download_file(task_id: str):
    try:
        db = SessionLocal()
        task = db.query(ImageTask).filter(ImageTask.id == task_id).first()
        db.close()

        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if task.status != "completed":
            raise HTTPException(status_code=400, detail=f"File not ready yet. Status: {task.status}")

        if not task.result_path:
            raise HTTPException(status_code=404, detail="Result file not found")

        # Генерируем временную ссылку на MinIO
        try:
            url = minio_client.presigned_get_object(
                MINIO_BUCKET,
                task.result_path,
                expires=timedelta(hours=1)
            )
            return RedirectResponse(url)
        except S3Error as e:
            log.error(f"MinIO error: {e}")
            raise HTTPException(status_code=500, detail="Error accessing file")

    except Exception as e:
        log.error(f"Download failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))