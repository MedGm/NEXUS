FROM python:3.12-slim

WORKDIR /app

COPY common ./common
RUN pip install --no-cache-dir -e ./common

COPY validator/requirements.txt ./requirements.txt
RUN sed -i 's|../common|./common|' requirements.txt && pip install --no-cache-dir -r requirements.txt

COPY validator/src ./src
COPY validator/expectations ./expectations

CMD ["python", "-m", "src.main"]
