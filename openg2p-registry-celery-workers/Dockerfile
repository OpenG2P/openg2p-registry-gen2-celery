FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY openg2p-registry-gen2-core/openg2p-registry-core /build/openg2p-registry-core
COPY openg2p-iam-service/iam-core /build/iam-core
COPY openg2p-registry-nsr-extensions/openg2p-registry-nsr-extension /build/openg2p-registry-nsr-extension
COPY openg2p-registry-gen2-celery/openg2p-registry-celery-workers /build/openg2p-registry-celery-workers
COPY openg2p-registry-gen2-celery/openg2p-registry-celery-beat-producers /build/openg2p-registry-celery-beat-producers

RUN pip install --upgrade pip setuptools wheel \
    && pip install \
        /build/openg2p-registry-core \
        /build/iam-core \
        /build/openg2p-registry-nsr-extension \
        /build/openg2p-registry-celery-workers \
        /build/openg2p-registry-celery-beat-producers

WORKDIR /app

CMD ["sh", "-c", "celery -A ${CELERY_APP:-openg2p_registry_celery_workers.main.celery_app} ${CELERY_OPTS:-worker --loglevel=info}"]
