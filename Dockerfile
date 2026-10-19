# ─── Stage 1: Install dependencies ke dalam .venv ────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install dependencies sistem yang dibutuhkan (cffi, cryptography, dll)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Buat virtualenv dan install package ke dalamnya
RUN python -m venv .venv \
    && .venv/bin/pip install --no-cache-dir --upgrade pip \
    && .venv/bin/pip install --no-cache-dir -r requirements.txt


# ─── Stage 2: Runtime image ───────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Salin .venv dari stage builder
COPY --from=builder /app/.venv .venv

# Salin kode bot
COPY bot.py .
COPY .env.example .

# Tambahkan .venv/bin ke PATH agar `python` langsung pakai venv
ENV PATH="/app/.venv/bin:$PATH"

# Bot membaca kredensial dari environment variable (set di panel VPS)
# .env tidak diperlukan jika env var sudah di-set di platform
CMD ["python", "bot.py"]
