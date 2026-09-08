FROM python:3.11.12-slim
WORKDIR /opt/bank
COPY pyproject.toml README.md ./
COPY bank ./bank
RUN pip install --no-cache-dir '.[runtime,streaming]'
COPY . .
RUN useradd --uid 50000 --gid 0 --create-home bank && chown -R 50000:0 /opt/bank
USER bank
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "bank.cli"]
