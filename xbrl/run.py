from sec_api import XbrlApi

xbrlApi = XbrlApi('hi')

url_10k_aapl = 'https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm'

aapl_xbrl_json = xbrlApi.xbrl_to_json(htm_url=url_10k_aapl)
print(aapl_xbrl_json)
