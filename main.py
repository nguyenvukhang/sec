from os import path
import json, os
from typing import Any
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame


parse_html = lambda x: BeautifulSoup(x.strip(), "html.parser")

CACHE_DIR = ".cache"
HEADERS = {"User-Agent": "someone@somewhere.com"}


class URL:
    CIK_LOOKUP_DATA = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"


def strip_base_url(url: str):
    if url.startswith("https://data.sec.gov"):
        return url[21:]
    if url.startswith("https://www.sec.gov"):
        return url[20:]
    raise Exception(
        f"Please fetch from either 'https://www.sec.gov' or 'https://data.sec.gov'."
    )


# in-memory data
mem = {"cik": {}}


class fetch:
    def __prep__(url: str):
        rel_path = strip_base_url(url)
        cache_filepath = path.join(CACHE_DIR, rel_path)
        os.makedirs(path.dirname(cache_filepath), exist_ok=True)
        return cache_filepath

    def __get__(url: str, plain_text=False):
        cache_filepath = fetch.__prep__(url)

        if path.isfile(cache_filepath):
            with open(cache_filepath, "r") as f:
                return f.read() if plain_text else json.load(f)

        print("[INFO] making fresh fetch!")

        # make the actual call to `www.sec.gov`
        response = requests.get(url, headers=HEADERS)

        # cache the response in text form
        with open(cache_filepath, "w") as f:
            f.write(response.text)

        return response.text if plain_text else response.json()

    def dataframe(url) -> DataFrame:
        cache_filepath = fetch.__prep__(url)

        if not path.isfile(cache_filepath):
            print("[INFO] making fresh fetch!")
            with requests.get(url, stream=True, headers=HEADERS) as r:
                r.raise_for_status()
                with open(cache_filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        print("reading filepath", cache_filepath)

        return pd.read_excel(cache_filepath, sheet_name=None)

    def text(url) -> str:
        return fetch.__get__(url, plain_text=True)

    def json(url) -> dict:
        return fetch.__get__(url, plain_text=False)


# requires the exact company name as found in the CIK lookup table
def get_cik(company_name: str):
    def parse_line(line: str):
        i = line.index(":")
        return (line[:i], line[i + 1 : -1])

    if not mem["cik"]:
        data = fetch.text(URL.CIK_LOOKUP_DATA).splitlines()
        data = map(parse_line, data)
        for k, v in data:
            mem["cik"][k] = v

    return mem["cik"][company_name]


class Company:
    def __init__(self, cik: str) -> None:
        self.cik = cik

    def get_filing_history(self) -> Any:
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        return fetch.json(url)

    def get_recent_filings(self, n=0) -> Any:
        fh = self.get_filing_history()
        recents = fh["filings"]["recent"]
        n = len(recents["accessionNumber"]) if n == 0 else n
        print("USING", n)
        keys = recents.keys()
        return [{k: recents[k][i] for k in keys} for i in range(n)]

    def find_filings(self, **kwargs):
        print(kwargs)
        rf = self.get_recent_filings()
        if len(kwargs.keys()) == 0:
            return rf
        return [f for f in rf if all([f[k] == v for k, v in kwargs.items()])]

    def fetch_form(self, filing: dict):
        print(filing)
        cik = self.cik
        an = filing["accessionNumber"].replace("-", "")
        slug = filing["primaryDocument"]
        url = path.join("https://www.sec.gov/Archives/edgar/data", cik, an, slug)
        return fetch.text(url)

    def fetch_financial_report(self, filing: dict) -> DataFrame:
        cik = self.cik
        an = filing["accessionNumber"].replace("-", "")
        url = path.join(
            "https://www.sec.gov/Archives/edgar/data", cik, an, "Financial_Report.xlsx"
        )
        return fetch.dataframe(url)


def get_tbl_title(soup: BeautifulSoup) -> str:
    while True:
        if not soup:
            return None
        t = (soup.text or "").strip()
        if len(t) == 0 or "\n" in t or "in millions" in t:
            soup = soup.previous_sibling
        else:
            break
    return soup.text


# annihilate all the useless stylez
def strip_attrs(soup: BeautifulSoup):
    for tag in soup.findAll(True):
        for attr in [a for a in tag.attrs]:
            del tag[attr]
    return soup


def innermost_text(soup: BeautifulSoup):
    for tag in soup.findAll(True):
        if len(tag.text.strip()) > 0:
            return tag.text


def extract_tbls(soup: BeautifulSoup) -> dict[str, BeautifulSoup]:
    tbls = [x for x in soup.find_all("table")]
    ht = {}
    for tbl in tbls:
        ht[get_tbl_title(tbl)] = strip_attrs(tbl)
    return ht


AAPL = Company(get_cik("APPLE COMPUTER INC"))
TSLA = Company(get_cik("TESLA, INC."))
NVDA = Company(get_cik("NVIDIA CORP"))
AMZN = Company(get_cik("AMAZON COM INC"))
COY = NVDA
filings = COY.find_filings(form="10-K")
df = COY.fetch_financial_report(filings[0])

def get_consolidated_balance_sheets(df: DataFrame):
    keys = df.keys()
    k ="Consolidated Balance Sheets"  
    if k in keys:
        return df[k]
    k = "CONSOLIDATED BALANCE SHEETS" 
    if k in keys:
        return df[k]

consolidated_balance_sheets = get_consolidated_balance_sheets(df)

print(df.keys())
print(consolidated_balance_sheets)
