import json
import os
import random
import requests

from src.parallelizer import Parallelizer
from bs4 import BeautifulSoup, Comment
from src.logger import Logger
from urllib import error, request


class Proxies:
    def __init__(self):
        self.timeout = 10
        self.url_max_retries = 5
        self.header_file = '/../json_data/header_details.json'
        self.proxy_file = '/../json_data/proxies.json'
        self.base_directory = os.path.join(os.path.dirname(__file__))
        self.data_header = json.load(open(self.base_directory + self.header_file, 'r'))
        self.data_proxies = json.load(open(self.base_directory + self.proxy_file, 'r'))
        self.logger = Logger('Scraping')

    def return_header(self, target='Random'):
        if target == 'Random':
            header = {
                'user-agent': self.data_header['user_agents_scrap'][
                    random.randint(0, len(self.data_header['user_agents_scrap']) - 1)],
                'referer': self.data_header['referrer'][random.randint(0, len(self.data_header['referrer']) - 1)],
                'Ugrade-Insecure-Requests': '0',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'en-US,en;q=0.5'}
        # https://stackoverflow.com/questions/49702214/python-requests-response-encoded-in-utf-8-but-cannot-be-decoded
        else:
            header = {'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"}
        return header

    def return_proxy(self, proxy, target='General'):
        if proxy is not None:
            proxy = proxy
        else:
            proxy = self.data_proxies[target][random.randint(0, len(self.data_proxies[target]) - 1)]
        proxies = {"http": "http://" + proxy,
                   "https": "http://" + proxy}
        return proxies, proxy

    def get_url(self, url, proxy=None, call='requests', target='General'):
        flag = self.url_max_retries

        while flag:
            header = self.return_header(target)
            proxies, proxy_current = self.return_proxy(proxy, target)
            try:
                if call == 'requests':
                    site = requests.get(url, headers=header, proxies=proxies, timeout=self.timeout)
                    if site.status_code == requests.codes.ok:
                        html_soup = BeautifulSoup(site.text, 'html.parser')
                        if len(html_soup.find_all('div', class_='alert alert-info')) == 0:
                            try:  # Try to avoid error when came contains strange characters
                                self.logger.logger.info(
                                    'Successful Get Request -> {} using Proxy -> {} '
                                    'on try-> {}'.format(url, proxy_current, self.url_max_retries - flag + 1))
                            except:
                                pass
                            flag = 0
                            return html_soup, proxy_current, site.status_code, site
                        else:
                            self.logger.logger.debug(
                                'Recapta Detected -> {} using Proxy -> {} on try-> {}'.format(url, proxy_current,
                                                                                              self.url_max_retries
                                                                                              - flag + 1))
                            flag -= 1
                            if flag == 0:
                                return '0', proxy_current, site.status_code, site
                    else:
                        self.logger.logger.debug(
                            'Proxy Status Mismatch -> {} using Proxy -> {} -> {} on try -> {}'.format(
                                site.status_code, proxy_current, url, self.url_max_retries - flag + 1))
                        flag -= 1
                        flag = 0 if site.status_code == 410 and flag == self.url_max_retries - 2 else flag
                        if flag == 0:
                            return '0', proxy_current, site.status_code, site
                elif call == 'urllib':
                    proxy_support = request.ProxyHandler(proxies)
                    opener = request.build_opener(proxy_support)
                    request.install_opener(opener)
                    req = request.Request(url, headers=header)
                    try:
                        page = request.urlopen(req, timeout=self.timeout)
                        html_soup = BeautifulSoup(page.read(), "lxml")
                    except error.HTTPError as e:
                        self.logger.logger.debug(
                            'Proxy Status Mismatch -> {} using Proxy -> {} -> {} on try -> {}'.format(
                                e.code, proxy_current, url, self.url_max_retries - flag + 1))
                        flag -= 1
                        if e.code == 403:
                            flag = 0
                        if flag == 0:
                            return '0', proxy_current, e.code, []
                    except error.URLError as e:
                        self.logger.logger.debug(
                            'Proxy Status Mismatch -> {} using Proxy -> {}  -> {} on try -> {}'.format(
                                e.reason, proxy_current, url, self.url_max_retries - flag + 1))
                        flag -= 1
                        if flag == 0:
                            return '0', proxy_current, 0, []
                    else:
                        try:  # Try to avoid error when came contains strange characters
                            self.logger.logger.info(
                                'Successful Get Request -> {} using Proxy -> {} '
                                'on try-> {}'.format(url, proxy_current,
                                                                            self.url_max_retries - flag + 1))
                        except:
                            pass
                        flag = 0
                        return html_soup, proxy_current, 200, page
            except Exception as E:
                self.logger.logger.debug(
                    'Something Went Wrong -> {} using  Proxy -> {} Error -> {} on try-> {}'.format(url, proxy_current,
                                                                                                   E,
                                                                                                   self.url_max_retries - flag + 1))
                flag -= 1
                if flag == 0:
                    return '0', proxy_current, -1, '0'

    def save_data(self, typ):
        if typ == 'Proxies':
            with open(self.base_directory + self.proxy_file, 'w') as outfile:
                json.dump(self.data_proxies, outfile, indent=4)
        elif typ == 'Header':
            with open(self.base_directory + self.header_file, 'w') as outfile:
                json.dump(self.data_header, outfile, indent=4)


class ProxyChecker(Proxies):
    def __init__(self):
        Proxies.__init__(self)
        Proxies.logger = Logger('Proxies')
        Proxies.max_workers = 50

    def fetch(self, proxy, check_url, name):
        _, _, site_code, _ = self.get_url(check_url, proxy=proxy, call='urllib', target=name)
        if site_code == 200:
            return proxy
        else:
            return '0'

    def check_proxies(self, check_url, name, proxy_list_urls):
        proxies_to_check = []
        self.data_proxies[name] = []
        for url in proxy_list_urls:
            for line in request.urlopen(
                    url):
                line = line.decode('utf-8').strip().split(':')
                proxies_to_check.append(line[2] + ':' + line[3] + '@' + line[0] + ':' + line[1] + '/')

        pa = Parallelizer(self.fetch, input_list=proxies_to_check, additional_arguments=(check_url, name),
                          max_workers=self.max_workers)
        pa.launch_parallelizer()
        self.data_proxies[name] = []
        [self.data_proxies[name].append(res) for res in pa.results if res]


if __name__ == "__main__":
    pro = ProxyChecker()
    pro.check_proxies('https://www.opply.io', 'General',
                          ['https://proxy.webshare.io/proxy/list/download/kflslydsitdymbuhlxuuundzlqyjorkxvikzckbi/-/http/username/direct/',
                           'https://proxy.webshare.io/proxy/list/download/bsktmhwzplbbgadyvstpcnjmiqymzpuelfegsike/-/http/username/direct/'])
    pro.save_data(typ='Proxies')
