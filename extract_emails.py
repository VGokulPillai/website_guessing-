import pandas as pd
import re
from bs4 import BeautifulSoup
from collections import deque
from src.proxies import Proxies
from urllib.parse import urlsplit

original_url = 'https://cota120.com/'
unscraped = deque([original_url])
scraped = set()
emails = set()
Pr = Proxies()

while len(unscraped):
    url = unscraped.popleft()
    scraped.add(url)

    parts = urlsplit(url)

    base_url = "{0.scheme}://{0.netloc}".format(parts)
    if '/' in parts.path:
        path = url[:url.rfind('/') + 1]
    else:
        path = url

    print("Crawling URL %s" % url)
    html_soup, _, site_code, response = Pr.get_url(url)
    if site_code == 200:
        new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.com", response.text, re.I))
        emails.update(new_emails)

        soup = BeautifulSoup(response.text, 'lxml')

        idx = 0
        for idx, anchor in enumerate(soup.find_all("a")):
            idx += 1
            if "href" in anchor.attrs:
                link = anchor.attrs["href"]
            else:
                link = ''

            if link.startswith('/'):
                link = base_url + link

            elif not link.startswith('http'):
                link = path + link

            if not link.endswith(".gz"):
                if not (link in unscraped or link in scraped) and original_url in link:
                    unscraped.append(link)

df = pd.DataFrame(emails, columns=["Email"])
