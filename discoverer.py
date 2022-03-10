
from src.database import *
from src.proxies import *


class Discoverer:
    def __init__(self, collection_to_scrape=None, collection_scraping=None):
        self.db = Database(collection_to_scrape=collection_to_scrape, collection_scraping=collection_scraping)
        self.documents_to_discover = []
        self.sites_to_discover = []
        self.max_workers = 10
        self.proxies = Proxies()

    def launch_europages(self, url):
        html_soup, _, _, _ = self.proxies.get_url(url)
        index = url.rfind('/')
        ps = html_soup.select('.page')
        max_pages = 1
        for p in ps:
            if p.get_text().isnumeric() and int(p.get_text()) > max_pages:
                max_pages = int(p.get_text())
        pages_to_discover = [*range(1, max_pages + 1)]
        self.sites_to_discover.extend([{"url": url[:index+1] + 'pg-' + str(page) + '/' + url[index+1:],
                                            "origin": 'Europages'} for page in pages_to_discover])

    def launch_discoverer(self, push_to_database=False):
        random.shuffle(self.sites_to_discover)
        pa = Parallelizer(self.discoverer_router, input_list=self.sites_to_discover, additional_arguments=[],
                          max_workers=self.max_workers)
        pa.launch_parallelizer()
        unique_ids = []
        unique_docs = []
        for doc in self.documents_to_discover:
            if doc['_id'] not in unique_ids:
                unique_docs.append(doc)
                unique_ids.append(doc['_id'])
        self.documents_to_discover = unique_docs
        if push_to_database:
            self.db.collection_to_scrape.insert_to_scrape(self.documents_to_discover)

    def discoverer_router(self, site):
        try:
            if site['origin'] == 'Organicbio':
                self.discover_organicbio(site)
            elif site['origin'] == 'Europages':
                self.discover_europages(site)
        except Exception as E:
            print(E)
            print(site['url'])

    def discover_europages(self, site):
        html_soup, _, _, _ = self.proxies.get_url(site['url'])
        links_on_page = [element.get('href') for element in html_soup.select('.company-name')]
        self.documents_to_discover.extend([{'discovered': 0, 'origin': 'Europages', 'url': link, '_id': link}
                                           for link in links_on_page])


if __name__ == "__main__":
    Di = Discoverer(db_to_scrape='test', collection_to_scrape='to_scrape',
                    db_scraping='test', collection_scraping='scraped')
    url = 'https://www.europages.co.uk/companies/Spain/almonds.html'
    Di.launch_europages(url)
    url = 'https://www.europages.co.uk/companies/Italy/almonds.html'
    Di.launch_europages(url)
    Di.launch_discoverer(push_to_database=False)
