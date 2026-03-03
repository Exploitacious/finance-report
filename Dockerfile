FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for numpy/pandas wheels)
# RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copy the application package explicitly
COPY app/ app/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for volume mounts to avoid permission collisions
RUN mkdir -p /app/data /app/tokens

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
