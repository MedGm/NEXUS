FROM python:3.12-slim

WORKDIR /app

COPY inference-api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY inference-api/src ./src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001"]
