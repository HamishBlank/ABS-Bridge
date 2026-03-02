FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libavahi-compat-libdnssd1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY db.py .
COPY static/ static/
COPY templates/ templates/

RUN mkdir -p /data

EXPOSE 8123
CMD ["python", "app.py"]