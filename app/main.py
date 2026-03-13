from fastapi import FastAPI

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