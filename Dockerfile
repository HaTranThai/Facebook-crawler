FROM python:3.10-slim

# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies, build tools, and required libraries
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    iputils-ping \
    g++ \
    protobuf-compiler \
    libprotobuf-dev \
    libnss3 \
    libxi6 \
    libxrender1 \
    libxext6 \
    libx11-6 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxfixes3 \
    libgbm1 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy project files into the container
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install pycld3 for language detection
RUN pip install --no-cache-dir -U pycld3

# Install Playwright and its dependencies
# RUN pip install playwright && playwright install --with-deps

# Install Google Chrome for Selenium
# RUN apt-get update && apt-get install -y wget unzip && \
#     wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt install -y ./google-chrome-stable_current_amd64.deb && \
#     rm google-chrome-stable_current_amd64.deb && \
#     apt-get clean

# Start the application using Uvicorn
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

HEALTHCHECK NONE

# Khởi chạy ứng dụng
CMD ["python", "Facebook.py"]