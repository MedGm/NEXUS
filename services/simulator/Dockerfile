FROM python:3.12-slim

WORKDIR /app

COPY common ./common
RUN pip install --no-cache-dir -e ./common

COPY simulator/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator/src ./src

CMD ["python", "-m", "src.main"]
