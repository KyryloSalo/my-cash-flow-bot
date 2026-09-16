FROM python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY bot/requirements.lock .
RUN pip install --no-cache-dir --require-hashes --no-build-isolation -r requirements.lock && pip check
COPY bot/ .
COPY runtime_schema /app/runtime_schema
CMD ["python", "-u", "app.py"]
