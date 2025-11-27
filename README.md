## 🧱 DLT – Data Loading Tool (JobTech API → DuckDB)

### Syfte
DLT-pipelinen hämtar realtidsdata från JobTech API för tre yrkesfält (**data**, **hr**, **ekonomi**) och lagrar det i en lokal DuckDB-databas (`job_ads_pipeline.duckdb`).

### Hur man kör
1. Installera beroenden:
   ```bash
   pip install -r requirements.txt
