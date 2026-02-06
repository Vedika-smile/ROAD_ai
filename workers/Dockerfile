FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ffmpeg \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install \
    --no-cache-dir \
    --timeout 300 \
    --retries 10 \
    -r requirements.txt


COPY process_video.py .
COPY models ./models

CMD ["python", "process_video.py"]
