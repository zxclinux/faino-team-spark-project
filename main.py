from fastapi import FastAPI
from config import settings
import uvicorn

app = FastAPI()

@app.get("/test")
async def test():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
