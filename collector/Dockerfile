FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py .

# Nicht als root laufen
RUN useradd -m appuser
USER appuser

CMD ["python", "-u", "collector.py"]
