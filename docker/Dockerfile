FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/

# Create data directories
RUN mkdir -p data/input data/output

# Default command
CMD ["python", "-c", "print('Container ready. Run: docker exec <container> python main.py')"]
