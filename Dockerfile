FROM python:3.12-slim

WORKDIR /app

# Only install dependencies needed for the monitor
RUN pip install --no-cache-dir \
    python-dotenv>=1.0.0 \
    google-api-python-client>=2.0.0 \
    google-auth-httplib2>=0.1.0 \
    google-auth-oauthlib>=1.0.0

# Copy only the files needed for the monitor
COPY config.json .
COPY run_monitor_loop.py .
COPY tools/read_google_sheet.py tools/
COPY tools/send_telegram.py tools/
COPY tools/monitor_7day_alerts.py tools/
COPY tools/__init__.py tools/

CMD ["python", "run_monitor_loop.py"]
