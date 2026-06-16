FROM python:3.11-slim
WORKDIR /app

# Pip paketlarni COPY dan oldin o'rnatish — Docker cache dan foydalanish uchun
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENTRYPOINT ["python", "main.py"]
