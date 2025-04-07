import requests
import pandas as pd
from io import StringIO

csvLinks = {
    "df1": "https://raw.githubusercontent.com/marukqs/techmuge/main/valstybiniai_numeriai_csv_part1.csv",
    "df2": "https://raw.githubusercontent.com/marukqs/techmuge/main/valstybiniai_numeriai_csv_part2.csv",
    "df3": "https://raw.githubusercontent.com/marukqs/techmuge/main/valstybiniai_numeriai_csv_part3.csv",
}

newFrames = {}

for name, url in csvLinks.items():
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(StringIO(response.text), header=None)
        df.columns = ["numeris"]
        df.to_csv(f"{name}.csv", index=False)
        newFrames[name] = df
        print(f"{name} loaded and saved: {df.shape[0]} rows")
    else:
        print(f"Failed to download {name}: HTTP {response.status_code}")
