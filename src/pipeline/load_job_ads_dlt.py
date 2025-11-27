import dlt
import requests

@dlt.source
def jobtech_source(occupation: str):
    @dlt.resource(name=f"job_ads_{occupation}", write_disposition="replace")
    def job_ads():
        url = "https://links.api.jobtechdev.se/joblinks"
        params = {"q": occupation, "limit": 100}
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        for ad in data.get("hits", []):
            municipality = None
            wa = ad.get("workplace_addresses")
            if isinstance(wa, list) and len(wa) > 0:
                municipality = wa[0].get("municipality")
            yield {
                "title": ad.get("headline"),
                "employer": ad.get("employer", {}).get("name"),
                "municipality": municipality,
                "occupation": occupation
            }
    return job_ads

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        destination="duckdb",
        dataset_name="job_ads_dataset"
    )

    occupations = ["data", "hr", "ekonomi"]
    for occ in occupations:
        src = jobtech_source(occ)
        pipeline.run(src)

    print("DLT-körning klar. Fil: job_ads_pipeline.duckdb")
