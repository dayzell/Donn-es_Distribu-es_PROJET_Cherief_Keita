FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer.py ./

# Exécution sans les droits root
RUN useradd --create-home app
USER app

CMD ["python", "consumer.py"]
