FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

RUN apt-get update \
	&& apt-get install -y --no-install-recommends build-essential \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
	&& pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

FROM python:3.11-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

RUN addgroup --system app \
	&& adduser --system --ingroup app app

COPY --from=builder /wheels /wheels
COPY requirements.txt ./
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
	&& rm -rf /wheels

COPY . .

USER app

EXPOSE 1337

CMD ["uvicorn", "main:api", "--host", "0.0.0.0", "--port", "1337"]
