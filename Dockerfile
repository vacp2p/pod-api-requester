FROM python:3.11.9-alpine AS base
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt
COPY api_requester.py utils.py configs.py kube_client.py common.py schemas.py app.py /app/

FROM base AS debug
WORKDIR /app
RUN apk add --no-cache \
    bash \
    bind-tools \
    curl \
    ethtool \
    iputils \
    jq \
    net-tools \
    tcpdump \
    vim \
    wget \
    ws \
    nodejs \
    npm \
  && npm install -g wscat

ENTRYPOINT ["sleep", "infinity"]

FROM base AS production
WORKDIR /app
ENTRYPOINT ["python", "./api_requester.py", "--mode", "server", "--config", "/mount/config.yaml"]