FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    git \
    bash \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instala Databricks CLI atual
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Copia requirements primeiro para aproveitar cache do Docker
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do projeto
COPY . .

CMD ["/bin/bash"]