from fastapi import FastAPI

app = FastAPI()


@app.get("/ingest")
def ingest():
    return {"message": "Hello World"}

@app.post("/insert")
def insert():
    return {"message": "Hello World"}