FROM python:3.13

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# Installa le dipendenze di sistema su Debian/Ubuntu
RUN apt-get update && apt-get install -y libmagic1 file gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

CMD ["python", "-u", "src/orchestrator.py"]