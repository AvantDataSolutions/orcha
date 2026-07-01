FROM python:3.12-slim-bookworm

WORKDIR /app

# Install orcha's runtime dependencies plus the test-only extras. Build context
# is the repo root (see docker-compose.yml) so both files are reachable.
COPY orcha/requirements.txt /tmp/orcha-requirements.txt
COPY orcha/tests/requirements.txt /tmp/test-requirements.txt
RUN pip install --no-cache-dir -r /tmp/orcha-requirements.txt -r /tmp/test-requirements.txt

ENV PYTHONUNBUFFERED=1

# The source is mounted at /app at runtime (see docker-compose volumes). The
# core suite runs as its own process against an initialised database.
ENTRYPOINT ["pytest", "orcha/tests/core"]
