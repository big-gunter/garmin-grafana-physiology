FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY server.py .
COPY auth.py .
COPY main.py .

# Persistent OAuth token storage
RUN mkdir -p /data

EXPOSE 8001

CMD ["python", "main.py"]
