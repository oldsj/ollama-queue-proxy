FROM python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN pip install --no-cache-dir uv==0.6.0 && \
    uv sync --frozen --no-dev --no-editable && \
    adduser --disabled-password --gecos "" --uid 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser
ENV PATH="/app/.venv/bin:${PATH}"
EXPOSE 11435
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:11435/health')" || exit 1
CMD ["ollama-queue-proxy"]
