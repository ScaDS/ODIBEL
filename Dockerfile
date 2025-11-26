FROM ghcr.io/astral-sh/uv:debian

WORKDIR app/
COPY . .
RUN uv python install 3.12
# Copy local .venv to container resolve symlinks
RUN if [ ! -d ".venv" ]; then uv venv; fi
RUN uv pip install -e .

CMD ["uv", "run", "pyodibel"]