FROM python:3.12-slim

WORKDIR /app

COPY incident-engine/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY incident-engine/src ./src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]
