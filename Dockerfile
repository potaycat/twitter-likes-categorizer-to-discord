FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    DB_PATH=/data/likes.db

# WORKDIR /app

COPY requirements.in .
RUN pip install --no-cache-dir -r requirements.in

COPY . .
RUN if [ ! -f .env ]; then \
      echo "ERROR: .env is missing from the Docker build context." >&2; \
      exit 1; \
    fi

EXPOSE 8000

CMD ["python", "app.py", "serve"]
