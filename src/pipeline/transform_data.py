import duckdb

# Anslut till databasen (skapar filen om den inte finns)
con = duckdb.connect("job_ads_pipeline.duckdb")

# Läs in och kör SQL-skriptet
with open("transform_data.sql", "r") as f:
    sql_script = f.read()
    con.execute(sql_script)

print("✅ Transformation klar – tabellen job_ads_summary skapad.")
print(con.execute("SELECT * FROM job_ads_dataset.job_ads_summary LIMIT 5").fetchdf())
