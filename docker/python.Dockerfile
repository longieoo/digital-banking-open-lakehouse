FROM python:3.11.12-slim
WORKDIR /opt/bank
COPY pyproject.toml README.md ./
COPY bank ./bank
RUN pip install --no-cache-dir '.[runtime,streaming]'
COPY . .
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "bank.cli"]
