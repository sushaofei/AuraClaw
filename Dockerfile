FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY migrations ./migrations

RUN useradd --create-home --uid 10001 auraclaw
USER auraclaw

ENTRYPOINT ["auraclaw"]
CMD ["serve", "--host", "0.0.0.0"]
