FROM python:3.12-slim

WORKDIR /app

COPY common ./common
RUN pip install --no-cache-dir -e ./common

COPY anomaly-detector/requirements.txt ./requirements.txt
RUN sed -i 's|../common|./common|' requirements.txt && pip install --no-cache-dir -r requirements.txt

COPY anomaly-detector/src ./src

CMD ["python", "-m", "src.main"]
