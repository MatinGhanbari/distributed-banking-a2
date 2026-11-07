# One image for every Python service (bank replicas, gateway, coordinator);
# compose selects the role with `command`.
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bank ./bank
COPY client.py .
EXPOSE 8000
CMD ["uvicorn", "bank.gateway:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
