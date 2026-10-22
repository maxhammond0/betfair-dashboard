FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py venue_states.csv ./
COPY static ./static

RUN useradd --create-home app
USER app

EXPOSE 8000
HEALTHCHECK --interval=60s --timeout=5s --start-period=60s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
