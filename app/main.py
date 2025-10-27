from fastapi import FastAPI
from app.api.routes import router

app = FastAPI()
app.include_router(router, prefix="/api/v1", tags=["energy"])


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "energy-api"}
