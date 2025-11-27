
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


RUN mkdir -p /root/.dbt
RUN echo "config:\n  send_anonymous_usage_stats: False\n  use_colors: True\njob_ads_dbt:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: /app/job_ads_pipeline.duckdb" > /root/.dbt/profiles.yml

COPY . .

EXPOSE 8501

RUN python load_job_ads_dlt.py
RUN python transform_data.py

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
