FROM python:3.12.10-slim


# Install dependencies
RUN apt-get update && apt-get install -y \
    wget unzip curl gnupg \
    && apt-get install -y chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set env vars for Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
WORKDIR /app
RUN chmod +x start.sh

CMD ["./start.sh"]

