FROM apache/airflow:2.5.1
FROM apache/airflow:2.5.1

# Устанавливаем системные зависимости для ijson (требуются для сборки)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-dev \
        gcc \
        libc-dev \
        && rm -rf /var/lib/apt/lists/*

# Возвращаемся к пользователю airflow
USER airflow

# Устанавливаем Python-пакеты
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install ijson pandas numpy matplotlib seaborn xgboost python-dotenv scikit-learn psycopg2-binary apache-airflow-providers-postgres sqlalchemy