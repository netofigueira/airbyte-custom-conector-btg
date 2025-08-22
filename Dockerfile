FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /airbyte/integration_code

# Copy source code
COPY source_btg ./source_btg
COPY main.py ./
COPY setup.py ./
COPY requirements.txt ./
COPY spec.json ./

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install .

# Labels for Airbyte
LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-btg

# Set entrypoint to use main.py directly
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]