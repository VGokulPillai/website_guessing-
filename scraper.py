from datetime import datetime
from src.database import *
from src.proxies import *
from unicodedata import normalize as uninorm

import PyPDF2
from io import BytesIO
import re


class Scraper:
    def __init__(self, collection_to_scrape=None, collection_scraping=None):
        self.documents_to_scrape = []
        self.db = Database(collection_to_scrape=collection_to_scrape, collection_scraping=collection_scraping)
        self.max_workers = 6
        self.proxies = Proxies()

    def load_to_scrape(self, origin, limit):
        self.documents_to_scrape.extend(
            self.db.collection_to_scrape.find({'discovered': 0, 'origin': origin}).limit(limit))

    def launch_scraper(self, update_to_scrape):
        random.shuffle(self.documents_to_scrape)
        pa = Parallelizer(self.scraper_router, input_list=self.documents_to_scrape,
                          additional_arguments=[update_to_scrape], max_workers=self.max_workers)
        pa.launch_parallelizer()

    def scraper_router(self, document, update_to_scrape):
        try:
            if document['origin'] == 'Organicbio':
                self.scrape_organicbio(document, update_to_scrape)
            elif document['origin'] == 'Europages':
                self.scrape_europages(document, update_to_scrape)
        except Exception as E:
            print(E)
            print(document['_id'])

    def scrape_organicbio(self, document, update_to_scrape):
        html_soup, _, _, _ = self.proxies.get_url(document['url'])

        tds = html_soup.select('th+ td')
        name = tds[0].get_text().strip()
        streetAddress = tds[1].get_text().strip()
        tds[3].a.decompose() if tds[3].a else tds[3]
        addressLocality = uninorm("NFKD", tds[3].get_text().strip())
        addressRegion = uninorm("NFKD", tds[4].get_text().strip())
        addressCountry = uninorm("NFKD", tds[5].get_text().strip())
        addressCountry = 'United States' if addressCountry == 'USA' else addressCountry
        postalCode = uninorm("NFKD", tds[2].get_text().strip())
        telephone = tds[6].get_text().strip()
        website = tds[9].get_text().strip()
        try:
            yearEstablished = int(tds[17].get_text().strip())
        except:
            yearEstablished = None
        facilitySize = tds[16].get_text().strip()
        mainActivity = tds[10].get_text(strip=True, separator=", ")
        while tds[19].a:
            tds[19].a.decompose()
        keywords_aux = tds[19].get_text(strip=True, separator="|").split('|') \
            if tds[19].get_text().strip() != '' else []
        keywords_aux = [uninorm("NFKD", k).rstrip(' -') for k in keywords_aux]
        # keywords = []
        # [keywords.extend(k.split('>')) for k in keywords_aux]
        keywords = [k.strip() for k in keywords_aux]
        keywords = list(set(keywords))
        certifications = [tds[15].get_text().strip()] if tds[15].get_text().strip() != '' else []
        description = tds[23].get_text().strip()
        registrationNumber = tds[13].get_text().strip()
        contactEmail = tds[8].get_text().strip()
        logo = html_soup.select('#content2 img')
        logoUrl = logo[0].get('src') if len(logo) > 0 else ''

        data = ScrapingEntry(_id=document['url'],
                             url=document['url'],
                             name=name,
                             lastScraped=datetime.now().timestamp(),
                             streetAddress=streetAddress,
                             addressLocality=addressLocality,
                             addressRegion=addressRegion,
                             addressCountry=addressCountry,
                             postalCode=postalCode,
                             telephone=telephone,
                             website=website,
                             mainActivity=mainActivity,
                             yearEstablished=yearEstablished,
                             facilitySize=facilitySize,
                             keywords=keywords,
                             certifications=certifications,
                             description=description,
                             registrationNumber=registrationNumber,
                             contactEmail=contactEmail,
                             logoUrl=logoUrl,
                             origin=document['origin']).data
        try:
            self.db.insert_document(data, update_to_scrape=update_to_scrape)
        except Exception as E:
            print(E)

    def scrape_europages(self, document, update_to_scrape):
        html_soup, _, _, _ = self.proxies.get_url(document['url'])

        data = {}
        if html_soup != '0':
            try:
                # Company content
                company_content = html_soup.select('.page__layout-sidebar--container-desktop')
                if company_content is not None:
                    # /// ID & URL
                    data["_id"] = document['url']
                    data["url"] = document['url']
                    data['lastScraped'] = datetime.now().timestamp()
                    data['name'] = self.get_item(html_soup.select('.company-content__company-name'))
                    data["category"] = ''
                    data["subcategory"] = ''
                    address_locality_pre = self.get_item(company_content[0].select('pre'))
                    data['postalAddress'] = {}
                    data['postalAddress']['streetAddress'] = ''
                    data['postalAddress']['addressLocality'] = ''
                    data['postalAddress']['addressRegion'] = ''
                    data['postalAddress']['addressCountry'] = self.get_item(company_content[0].select('.flag-1x+ span'))
                    data['postalAddress']['postalCode'] = ''
                    data['postalAddress']['addressRaw'] = address_locality_pre.replace('\n', ';').replace(' ;', '; ')\
                        if address_locality_pre is not None else ''
                    data['telephone'] = self.get_item(html_soup.find("span", {"class": "js-num-tel js-hidden"}))
                    website_pre = html_soup.find("a", {"class": "page-action", "itemprop": "url"})
                    data['website'] = website_pre.get('href') if website_pre is not None else ''
                    data['website'].lower() \
                        .replace('http://http://', 'http://'). \
                        replace('https://https://', 'https://'). \
                        replace('https://http://', 'http://'). \
                        replace('http://https://', 'https://')

                    data['yearEstablished'] = data['locationType'] = data['mainActivity'] = None
                    organization_information = html_soup.find(class_="organisation-list")
                    if organization_information is not None:
                        for info in organization_information.find_all("li", recursive=False):
                            info_header = info.find_all("span")[0].get_text()
                            if info_header == 'Year established':
                                # ///Year established
                                data['yearEstablished'] = int(self.get_item(info.find_all("span")[-1]))
                            elif info_header == 'Site status':
                                # ///Site status
                                data['locationType'] = self.strip_text(self.get_item(info.find_all("span")[-1])).replace(
                                    ',',
                                    ';')
                            elif info_header == 'Main activity':
                                # ///Main activity
                                data['mainActivity'] = self.strip_text(self.get_item(info.find_all("span")[-1]))
                                if html_soup.find(class_="label-dropDown"):
                                    for info_add in info.find_all("li"):
                                        data['mainActivity'] += '; ' + self.get_item(info_add)

                    data['vatId'] = self.get_item(company_content[0].find(itemprop="vatID"))
                    data['revenue'] = self.get_item(html_soup.find("div", {"class": "data sprite icon-key-ca"}))
                    data['employeesSite'] = self.get_item(
                        html_soup.find("div", {"class": "data sprite icon-key-people"}))

                    products_subpage = html_soup.find(title='Products')
                    if products_subpage is not None:
                        urllink_products_orig = products_subpage.get('href')
                    documents_subpage = html_soup.find(title='Documents')
                    if documents_subpage is not None:
                        urllink_documents_orig = documents_subpage.get('href')
                    data['products'] = []
                    if products_subpage is not None:
                        product_page = 0
                        while True:
                            product_page += 1
                            urllink_products = urllink_products_orig + '?page=' + str(product_page)
                            subpage_products, _, _, _ = self.proxies.get_url(urllink_products)
                            products_overview = subpage_products.select('.products-list--item')
                            for product in products_overview:
                                product_info1 = product.select('.ellipsis a')
                                product_info2 = product.select('.no-h')
                                product_add = ''
                                product_add += product_info1[0].text if len(product_info1) > 0 else ''
                                product_add += ' (' + product_info2[0].text + ')' if len(product_info2) > 0 else ''
                                try:
                                    data['products'].append(product_add)
                                except:
                                    pass

                            if len(subpage_products.select('.page+ .prevnext')) == 0:
                                break
                            if product_page > 30:
                                break
                    data['origin'] = 'Europages'
                    # Keyword groups
                    keyword_groups = html_soup.find_all(class_="keyword-tag")
                    # ///Associated keywords
                    data['keywords'] = []
                    for keyword_group in keyword_groups:
                        data['keywords'].extend(list(keyword_group.descendants)[2::3])

                    # ///Company description
                    description_aux = re.sub(' +', ' ',
                                             self.get_item(
                                                 html_soup.select(".company-description")) \
                                             .replace('\n', ' '))
                    data['description'] = description_aux.split("Other companies in the same industry")[0]
                    #/// Brands
                    brands_aux = html_soup.find(class_="brand-list")
                    data['brands'] = []
                    if brands_aux is not None:
                        for item in html_soup.find(class_="brand-list").findAll('li'):
                            data['brands'].append(item.text)

                    incoterms = html_soup.select(".u-biggest")
                    data['incoterms'] = []
                    if len(incoterms) != 0:
                        for term in incoterms:
                            data['incoterms'].append(term.text)
 
                    data['documents'] = []
                    if documents_subpage is not None:
                        subpage_documents, _, _, _ = self.proxies.get_url(urllink_documents_orig)
                        docs = subpage_documents.select('.js-click-out')
                        for doc in docs:
                            try:
                                data['documents'].append(
                                    {"document": doc.select('.label')[0].text.replace('\xa0', ' '),
                                     "url": doc.a.get('href')})
                            except:
                                pass

                    data['logoUrl'] = ''
                    logoURL_aux = html_soup.select('.company-logo--container img')
                    if len(logoURL_aux) != 0:
                        data['logoUrl'] = logoURL_aux[0].get('src')
                    data['contactEmails'] = []
                    data['companydataEnriched'] = []
                    if data['website'] != '':
                        data['companydataEnriched'].append(Website(data['website'], data['origin']).data)
            except Exception as E:
                print(E)
                data = {}
            try:
                self.db.insert_document(data, update_to_scrape=update_to_scrape)
            except Exception as E:
                print(E)

    def get_item(self, item):
        if item is not None and len(item) != 0:
            if isinstance(item, list):
                return item[0].get_text(strip=True)
            else:
                return item.get_text(strip=True)
        else:
            return ''

    def strip_text(self, text):
        return re.sub(' +', ' ', text.translate(str.maketrans("\n\t\r", "   ")))

    def scrape_website(self, url, max_attempts=3):
        session = requests.session()
        current_attempt = 1

        while current_attempt < max_attempts:
            header = self.proxies.return_header()
            proxies, proxy_current = self.proxies.return_proxy(None)
            try:
                site = session.get(url, timeout=10, headers=header, proxies=proxies)
                if site.status_code == requests.codes.ok:
                    if url.endswith('.pdf'):
                        return site.content
                    else:
                        html_soup = BeautifulSoup(site.text, 'html.parser')

                        # Check whether reCAPTCHA was detected
                        if len(html_soup.find_all('div', class_='alert alert-info')) == 0:
                            return html_soup
                        else:
                            print('reCAPTCHA detected when scraping ' + url)

            except Exception as e:
                print(e)

            current_attempt += 1

        print('Something went wrong when scraping ' + url)
        return None

    @staticmethod
    def get_data_from_scraping(content, b_text=True, b_images=False):
        text, images = None, None
        if not content:
            return text, images

        if b_text:
            if isinstance(content, BeautifulSoup):
                html_soup_text = content.findAll(text=lambda text: not isinstance(text, Comment))
                html_soup_text_visible = '\n'.join(list(filter(is_visible, html_soup_text)))
                text = html_soup_text_visible.split()
            elif isinstance(content, bytes):
                with BytesIO(content) as data:
                    try:
                        read_pdf = PyPDF2.PdfFileReader(data)
                    except PyPDF2.utils.PdfReadError as err:
                        print(f'Error while reading a PDF file: {err}')

                    text = []
                    for page in range(read_pdf.getNumPages()):
                        page_text = read_pdf.getPage(page).extractText()
                        text.extend(page_text.split())

        if b_images:
            if isinstance(content, BeautifulSoup):
                # Extracting image links only
                images = [img.attrs.get("src").strip() for img in content.find_all('img') if img.attrs.get("src")]

        return text, images


# Filtering for visible text
def is_visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    else:
        return True


if __name__ == "__main__":
    Sc = Scraper(collection_to_scrape='to_scrape', collection_scraping='scraped')
    # Sc.load_to_scrape(origin='Organicbio', limit=7000)
    # Sc.launch_scraper(update_to_scrape=True)

    url = "https://www.tidefordorganics.com/blog/miso-morning"
    # url = "https://www.everland.ca"
    html_soup = Sc.scrape_website(url)
    text, _ = Sc.get_data_from_scraping(html_soup)
    if text:
        print(text)
