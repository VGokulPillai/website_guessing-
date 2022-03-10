from bson.objectid import ObjectId
from collections import Counter
from collections import deque
from datetime import datetime
from src.mongoDB import client
from src.parallelizer import *
from src.searchengine_scraper import *
from urllib import parse as urlparse

import re
import uuid


class DataEnrichment:
    def __init__(self):
        self.scraper = Yahoo()
        self.websites_from_searchengine = []
        self.emails_from_website = []

    def website_from_search_engine(self, company_name, city=None, country=None):
        search_term = company_name
        if city is not None:
            search_term += ' ' + city
        if country is not None:
            search_term += ' ' + country
        links, _ = self.scraper.retrieve_results_yahoo(search_term, typ='single', max_recall=2)
        return [Website(link, 'Yahoo').data for link in links]

    def email_from_website_launcher(self, website_list):
        pa = Parallelizer(self.email_from_website, input_list=website_list, additional_arguments=(),
                          max_workers=500)
        pa.launch_parallelizer()
        return [res for res in pa.results]

    def email_from_website(self, website):
        unscraped = deque([website])
        scraped = set()
        emails = set()
        scrape_counter = 0
        while len(unscraped):
            print(unscraped)
            url = unscraped.popleft()
            scraped.add(url)
            parts = urlparse.urlsplit(url)
            base_url = "{0.scheme}://{0.netloc}".format(parts)
            if '/' in parts.path:
                path = url[:url.rfind('/') + 1]
            else:
                path = url

            html_soup, _, site_code, response = self.scraper.get_url(url, call='requests')
            if site_code != 200:
                html_soup, _, site_code, response = self.scraper.get_url(url, call='urllib')
            if site_code == 200:
                scrape_counter += 1

                new_emails = set(re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', response.text, re.I))
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
                        if not (link in unscraped or link in scraped) and base_url in link:
                            unscraped.append(link)
            if scrape_counter > 30:
                break
        return [Email(email, 'Website', website).data for email in list(emails)]


class Website:
    def __init__(self, website, origin):
        now = datetime.now()
        if urlparse.urlparse(website) is not None:
            if website[0:4] == 'www.':
                url_root = website.lstrip('www.')
            else:
                if urlparse.urlparse(website).hostname is not None:
                    try:
                        url_root = urlparse.urlparse(website).hostname.lstrip('www.')
                    except Exception as E:
                        url_root = website
                        print(E)
                else:
                    url_root = website
        else:
            if website[0:4] == 'www.':
                try:
                    url_root = website.lstrip('www.')
                except Exception as E:
                    print(E)
                    url_root = website
            else:
                url_root = website
        self.data = {'url': website, 'url_root': url_root, 'origin': origin,
                     'probability': float("nan"), 'dateCreated': now, 'lastModified': now}


class Email:
    def __init__(self, email, type=None, source=None):
        self.data = {'id': ObjectId(), 'email': email, 'origin': {'_type': type, 'source': source},
                     'status': 'Unclear', 'dateCreated': datetime.now()}


def website_probability():
    supplier_database = client['supplier_scraping_V2']['def_supplier_database_V2']
    supplier_database_list = list(supplier_database.find({'companydataEnriched': {'$gt': {'$size': 0}}},
                                                         {'companydataEnriched': 1}).limit(1000000))
    data = []
    url_roots = []
    for idx, record in enumerate(supplier_database_list):
        print(idx)
        #data.append({'_id': record['_id'], 'companydataEnriched': record['companydataEnriched']})
        for r in record['companydataEnriched']:
            url_roots.append(r['url_root'])
    url_roots_counted = Counter(url_roots)
    url_roots_counted_relative = {key: url_roots_counted[key]/len(url_roots) for key in url_roots_counted}
    # If coming from a directory, probability is 0.7
    # If coming from Yahoo, probability according to distribution of root_urls


if __name__ == "__main__":
    de = DataEnrichment()
    de.website_from_search_engine(company_name='Global Concentrate Inc', country='United States')
