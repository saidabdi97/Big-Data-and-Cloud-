import requests
import pandas as pd
from datetime import datetime

# API
url = "https://links.api.jobtechdev.se/joblinks"

print("Välkommen till JobbSök (JobTech API)\n")
occupation = input("Ange vilket jobb du vill söka efter: ")

params = {"q": occupation, "limit": 10}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    print(f"\n{data['total']['value']} träffar hittades för '{occupation}' ({datetime.now().strftime('%H:%M:%S')})\n")
    hits = data.get("hits", [])
    if not hits:
        print("Inga jobb hittades, testa ett annat sökord.")
    else:
        df = pd.json_normalize(hits)
        for i, row in df.iterrows():
            print(f"🔹 {row.get('headline')} — {row.get('employer.name')}")
            print(f"    {row.get('workplace_addresses[0].municipality')}\n")
else:
    print(f"API-anrop misslyckades. Statuskod: {response.status_code}")
