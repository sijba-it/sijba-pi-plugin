FROM node:22-slim

RUN npm install -g --ignore-scripts @earendil-works/pi-coding-agent \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip git ca-certificates \
        curl wget jq ripgrep unzip less nano openssh-client sqlite3 procps \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --break-system-packages --no-cache-dir websockets

WORKDIR /app
COPY main.py .

# HOME is set to /work (the project's own bind-mounted directory, see
# sijba-agent-controller/catalog.json's env override for this plugin) so
# pi's own state (~/.pi/agent/sessions, models-store.json) persists across
# container restarts via the same bind mount WORK_DIR already uses —
# without this, every `docker run --rm` respawn would start pi with a
# fresh, empty $HOME and lose all session continuity.
ENTRYPOINT ["python3", "/app/main.py"]
