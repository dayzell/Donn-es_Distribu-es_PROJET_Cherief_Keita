FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulator.py ./

# Exécution sans les droits root
RUN useradd --create-home app
USER app

# -u : logs non bufferisés, visibles immédiatement dans "docker compose logs"
CMD ["python", "-u", "simulator.py"]
