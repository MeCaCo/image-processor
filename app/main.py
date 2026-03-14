from fastapi import FastAPI
from fastapi import UploadFile, File
import uuid
from datetime import datetime

app = FastAPI(
    title="Image Processing Service",
    description="Async image processing with Go workers",
    version="0.1.0"
)

@app.get("/")
async def root():
    return {"message": "Image Processing API", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Просто сохраняем информацию о файле
    file_id = str(uuid.uuid4())

    return {
        "file_id": file_id,
        "filename": file.filename,
        "size": 0,
        "status": "uploaded",
        "created_at": datetime.now().isoformat()
    }


@app.get("/files")
async def list_files():
    # Заглушка для списка файлов
    return {"files": [], "message": "Список файлов будет здесь"}