FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY report_template.html .

ENV PORT=8080
ENV PYTHONUNBUFFERED=1
#real time outout


# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "main:app"]
#0.0.0.0 all interfaces port 8080
#loads app object from main.py