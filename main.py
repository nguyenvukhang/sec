from os import path
import urllib
import json, os, re
from typing import Any
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
import re

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
            with open(cache_filepath, "r", encoding="utf-8") as f:
                return f.read() if plain_text else json.load(f)

        print("[INFO] making fresh fetch!")

        # make the actual call to `www.sec.gov`
        response = requests.get(url, headers=HEADERS)

        # cache the response in text form
        with open(cache_filepath, "w", encoding="utf-8") as f:
            f.write(response.text)

        return response.text if plain_text else response.json()

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


def filter_filings(rf, **kwargs):
    return [f for f in rf if all([f[k] == v for k, v in kwargs.items()])]


class Company:
    def __init__(self, cik: str) -> None:
        self.cik = cik

    def get_filing_history(self) -> Any:
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        return fetch.json(url)

    def get_filings(self, filings, n=0) -> list[Any]:
        keys = [x for x in filings.keys()]
        m = len(filings[keys[0]])
        n = m if n == 0 else min(n, m)
        return [{k: filings[k][i] for k in keys} for i in range(n)]

    def get_recent_filings(self, n=0) -> list[Any]:
        fh = self.get_filing_history()
        recents = fh["filings"]["recent"]
        return self.get_filings(recents, n)

    def get_all_filings(self, **kwargs) -> list[Any]:
        fh = self.get_filing_history()
        hist = fh["filings"]["files"]
        PRE = "https://data.sec.gov/submissions/"
        lofilings = [fetch.json(PRE + v["name"]) for v in hist]
        lofilings = [self.get_filings(f) for f in lofilings]
        filings = []
        [filings.extend(f) for f in lofilings]
        filings.extend(self.get_filings(fh["filings"]["recent"]))
        return filter_filings(filings, **kwargs)

    def find_filings(self, **kwargs):
        f = self.get_all_filings()
        if len(kwargs.keys()) == 0:
            return f
        f = [f for f in f if all([f[k] == v for k, v in kwargs.items()])]
        f.sort(key=lambda v: v["reportDate"], reverse=True)
        return f

    def fetch_form(self, filing: dict):
        cik = self.cik
        an = filing["accessionNumber"].replace("-", "")
        slug = filing["primaryDocument"]
        # ugly. FIXME
        url = urllib.parse.urljoin("https://www.sec.gov/Archives/edgar/data/", cik) + '/'
        url = urllib.parse.urljoin(url, an) + '/'
        url = urllib.parse.urljoin(url, slug)
        return fetch.text(url)


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


def get_consolidated_balance_sheets(df: DataFrame):
    keys = df.keys()
    k = "Consolidated Balance Sheets"
    if k in keys:
        return df[k]
    k = "CONSOLIDATED BALANCE SHEETS"
    if k in keys:
        return df[k]


class Fin:
    NON_ALPHA = re.compile("[\W_]+")
    C_STATEMENTS_OF_OPS = "consolidatedstatementsofoperations"
    C_STATEMENTS_OF_COMPREHENSIVE_INCOME = "consolidatedstatementsofcomprehensiveincome"
    C_BALANCE_SHEETS = "consolidatedbalancesheets"
    C_STATEMENTS_OF_STOCKHOLDERS_EQUITY = "consolidatedstatementsofstockholdersequity"
    normalize = lambda v: Fin.NON_ALPHA.sub("", v).lower()
    matches = lambda q: lambda v: Fin.normalize(v) == q


# (<# rows>, <# cols>)
def dimensions(tbl: BeautifulSoup) -> tuple[int, int]:
    rows = tbl.find_all("tr")
    for row in rows:
        cols = row.find_all(recursive=False)
        return (len(rows), len(cols))
    return (len(rows), 0)


def init_empty_data(rows: int, cols: int) -> list[list[str]]:
    return [["" for _ in range(cols)] for _ in range(rows)]


# remove blank rows and useless columns
def remove_blank_ranks(data: list[list[str]]):
    blank_r = lambda r: all([r == "" for r in r])
    data = [r for r in data if not blank_r(r)]

    if len(data) == 0:
        return data

    col_num = len(data[0])
    row_num = len(data)
    R, C = range(row_num), range(col_num)

    cols_to_rm = []
    for c in C:
        # if it's either
        #     1. a blank, or
        #     2. the cell on the right has the same content
        # then it's considered useless
        #
        # if whole col is useless, remove it
        all_useless = True
        for r in R:
            text = data[r][c]
            useless = text == ""
            if c + 1 < len(data[r]):
                useless |= data[r][c + 1] == text
            if not useless:
                all_useless = False
                break
        if all_useless:
            cols_to_rm.append(c)
    return [[data[r][c] for c in C if c not in cols_to_rm] for r in R]


# split a table by dominating rows (if one row is filled with the same content)
def split_subtables(tbl: list[list[str]]) -> list[list[str]]:
    row_num = len(tbl)
    if row_num == 0:
        return tbl
    col_num = len(tbl[0])
    if col_num == 0:
        return tbl
    R, C = range(row_num), range(col_num)
    result = {}
    buf = []
    name = "<start>"
    for r in R:
        if all([tbl[r][c] == tbl[r][0] for c in C]):
            result[name] = buf
            buf = []
            name = tbl[r][0]
        else:
            buf.append(tbl[r])
    result[name] = buf
    return result


def read_table(tbl: BeautifulSoup) -> list[list[str]]:
    if not tbl:
        return None
    dim = dimensions(tbl)
    data = init_empty_data(*dim)

    rows = [r for r in tbl.find_all("tr")]
    for r in range(len(rows)):
        cols = [c for c in rows[r].find_all(recursive=False)]
        j = 0
        for c in range(len(cols)):
            # print(r, c)
            colspan = int(cols[c].get("colspan", "1"))
            for _ in range(colspan):
                data[r][j] = cols[c].text.strip()
                j += 1

    data = remove_blank_ranks(data)
    # data = split_subtables(data) # optional. WARNING: will change data structure
    return data


def soup_find_table(soup: BeautifulSoup, query: str, index=-1) -> BeautifulSoup:
    titles = [x for x in soup.find_all(string=Fin.matches(query))]
    if not titles:
        return None
    # use the last instance of it appearing alone because the first few might
    # be part of the table of contents
    #
    # (snagged on AMZN's 10-K)
    return titles[index].find_next("table")


AAPL = Company(get_cik("APPLE COMPUTER INC"))
TSLA = Company(get_cik("TESLA, INC."))
NVDA = Company(get_cik("NVIDIA CORP"))
AMZN = Company(get_cik("AMAZON COM INC"))
BB = Company(get_cik("BLACKBERRY LTD"))
COY = NVDA
filings = COY.find_filings(form="10-K")
print([v["filingDate"] for v in filings])
most_recent_10K = filings[0]
soup = parse_html(COY.fetch_form(most_recent_10K))
tbl_soup = soup_find_table(soup, Fin.C_BALANCE_SHEETS)
# tbl_soup = soup_find_table(soup, Fin.C_STATEMENTS_OF_OPS)
tbl = read_table(tbl_soup)
print(tbl)
