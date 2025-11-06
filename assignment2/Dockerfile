FROM python:3.10-slim

WORKDIR /app

# Install dependencies, net-tools used for health check.
RUN pip install rpyc requests && \
    apt-get update && \
    apt-get install -y net-tools && \
    rm -rf /var/lib/apt/lists/*

# Necessary python files.
COPY coordinator.py worker.py ./

# Default command.
CMD ["python", "worker.py"]

