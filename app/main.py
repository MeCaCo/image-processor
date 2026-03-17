from fastapi import FastAPI, UploadFile, File, HTTPException
import uuid
from datetime import datetime
import os
import shutil
from pathlib import Path

from models import ImageTask
from database import SessionLocal

app = FastAPI(
    title="Image Processing Service",
    description="Async image processing with Go workers",
    version="0.1.0"
)

# Создаем папку для загрузок
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@app.get("/")
async def root():
    return {"message": "Image Processing API", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Генерируем ID
        file_id = str(uuid.uuid4())

        # Сохраняем файл локально
        file_path = UPLOAD_DIR / f"{file_id}_{file.filename}"

        # Копируем файл
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Получаем размер файла
        file_size = file_path.stat().st_size

        # Сохраняем в БД
        db = SessionLocal()
        task = ImageTask(
            id=file_id,
            filename=file.filename,
            status="uploaded",
            original_path=str(file_path)
        )
        db.add(task)
        db.commit()
        db.close()

        return {
            "file_id": file_id,
            "filename": file.filename,
            "size": file_size,
            "status": "uploaded",
            "created_at": datetime.now().isoformat(),
            "path": str(file_path)
        }

    except Exception as e:
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