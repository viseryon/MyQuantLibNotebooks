import asyncio
import logging
from datetime import datetime
from io import StringIO
from itertools import product

import aiofiles
import httpx
import pandas as pd
from aiolimiter import AsyncLimiter
from anyio import Path

logger = logging.getLogger()
logger.addHandler(logging.FileHandler("logs"))

BONDSPOT_LINK = "https://www.bondspot.pl/fixing_obligacji"
REQUESTS_MAX_RATE = 5

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "pl,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,de-DE;q=0.6,de;q=0.5",
    "connection": "keep-alive",
    "cookie": "lang_code=PL; PHPSESSID=086n6bngl7trua01408o44ph94",
    "dnt": "1",
    "host": "www.bondspot.pl",
    "referer": "https://www.bondspot.pl/fixing_obligacji",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
}


async def make_request(
    client: httpx.AsyncClient,
    date: str,
    fixing: int,
    limiter: AsyncLimiter,
) -> httpx.Response:
    async with limiter:
        params = {"date": date, "type": fixing}
        print(f"requesting {date} - {params = }")
        resp = await client.get(
            BONDSPOT_LINK,
            params=params,
            headers=HEADERS,
        )
        async with aiofiles.open(Path("htmls", f"{date}.html")) as f:
            await f.write(resp.text)

        return resp


async def get_data_async(
    start_date: str | datetime = "2000-01-01",
    end_date: str | datetime | None = None,
) -> list[httpx.Response]:
    if end_date is None:
        end_date = datetime.today()

    dates = [date.strftime("%Y%m%d") for date in pd.date_range(start_date, end_date, freq="B")]

    limiter = AsyncLimiter(max_rate=REQUESTS_MAX_RATE, time_period=1)
    async with (
        httpx.AsyncClient(
            timeout=60.0,
        ) as client,
        asyncio.TaskGroup() as tg,
    ):
        tasks = [
            tg.create_task(make_request(client=client, date=date, fixing=fixing, limiter=limiter))
            for date, fixing in product(dates, (1, 2))
        ]

    return [task.result() for task in tasks]


def filter_dfs(results: list[httpx.Response]):
    dfs = []
    for result in results:
        html = result.text
        params = result.url.params
        date = params["date"]
        fixing = params["type"]
        try:
            df = pd.read_html(StringIO(html))[2]
        except Exception as e:
            logger.error("%s - %s", date, e)
        else:
            df["date"] = date
            df["fixing"] = fixing
            dfs.append(df)
    return dfs


def main() -> None:
    coroutine = get_data_async()
    results = asyncio.run(coroutine)

    data = pd.concat(filter_dfs(results))

    data = (
        data.replace({"-": pd.NA, "KURS NIEOKREŚLONY": pd.NA})
        .rename(
            {
                "Nazwa": "Seria",
                "ISIN": "Kod ISIN",
                "Cena K": "buy_price",
                "Cena S": "sell_price",
                "Rent.K": "buy_yield",
                "Rent.S": "sell_yield",
                "Cena fix": "fix_price",
                "Rent fix": "fix_yield",
                "date": "Date",
                "fixing": "Fixing",
            },
            axis=1,
        )
        .astype({
            "Seria": "string",
            "Kod ISIN": "string",
            "Fixing": "Int8",
            "buy_price": "Float32",
            "sell_price": "Float32",
            "buy_yield": "Float32",
            "sell_yield": "Float32",
            "fix_price": "Float32",
            "fix_yield": "Float32",
        })[
            [
                "Date",
                "Seria",
                "Kod ISIN",
                "Fixing",
                "buy_price",
                "sell_price",
                "buy_yield",
                "sell_yield",
                "fix_price",
                "fix_yield",
            ]
        ]
    )

    path = Path(__file__).parent / Path("data")
    data.to_csv(path / "bond_prices.csv", index=False)
    data.to_parquet(path / "bond_prices.parquet", index=False)


if __name__ == "__main__":
    main()
