FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tracker.py .
COPY templates ./templates
COPY static ./static

CMD ["python", "-u", "tracker.py"]