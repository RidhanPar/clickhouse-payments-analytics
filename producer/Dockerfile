FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .

# The producer touches /tmp/heartbeat after every successful flush; the
# healthcheck fails if no flush landed in the last 30 seconds.
HEALTHCHECK --interval=10s --timeout=3s --start-period=45s --retries=3 \
  CMD python -c "import os, time, sys; sys.exit(0 if time.time() - os.path.getmtime('/tmp/heartbeat') < 30 else 1)"

CMD ["python", "-u", "producer.py"]
