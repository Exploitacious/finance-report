from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Finance Report API is running"}

@app.get("/report")
def get_report():
    return {"report_data": "Financial data would go here"}
