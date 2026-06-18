FROM pearsonwhite/logos-core-dst:wip1-amd AS base

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python-is-python3 && \
    rm -rf /var/lib/apt/lists/*

USER nixuser

# This is typically created when running logoscore.
# Since we're adding the tokens before running logoscore, we must manually create it.
RUN mkdir -p /home/nixuser/.logoscore/client/

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt
RUN pip install --no-cache-dir --break-system-packages git+https://github.com/logos-co/logos-logoscore-py.git@5842bd32b9b5529abf017a1e9032dee988d8180b
COPY api_requester.py \
    async_client.py \
    utils.py \
    configs.py \
    kube_client.py \
    common.py \
    shadow_resolver.py \
    schemas.py \
    app.py \
    /app/
COPY routers/ /app/routers/

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