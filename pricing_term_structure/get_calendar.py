import re
from pathlib import Path

import httpx
import pandas as pd

resp = httpx.get("https://www.gov.pl/web/finanse/kupony")
hash_ = re.search(r'href="/attachment/([\w-]+)"', resp.text).groups()[0]
url = f"https://www.gov.pl/attachment/{hash_}"


bond_cal = (
    pd.read_excel(
        url,
        header=[0, 1],
        sheet_name="ObligacjeStałoprocentowe",
    )
    .rename(
        columns={
            "Unnamed: 0_level_0": "Info",
            "Unnamed: 1_level_0": "Info",
            "Unnamed: 2_level_0": "Info",
            "Unnamed: 3_level_0": "Info",
        },
    )
    .replace({"-": pd.NA})
)


info = bond_cal.loc[:, ["Info"]].stack(level=0, future_stack=True).reset_index(1, drop=True)
calendar = (
    bond_cal.loc[:, [f"Kupon Nr {num}" for num in range(1, 32)]]
    .stack(level=0, future_stack=True)
    .reset_index(1)
)
df = info.join(calendar).dropna(how="any").rename(columns={"level_1": "Numer okresu"})
df["Numer okresu"] = df["Numer okresu"].str[9:]
df = df.astype({
    "Początek okresu": "datetime64[ns]",
    "Koniec okresu": "datetime64[ns]",
    "Dzień ustalenia praw": "datetime64[ns]",
    "Data wymagalności": "datetime64[ns]",
    "Odsetki (PLN)": "float64",
    "Numer okresu": "int16",
    "Kod ISIN": "string",
    "Seria": "string",
})

path = Path(__file__).parent / Path("data", "kalendarz_odsetkowy.parquet")

df.to_parquet(path, index=False)
