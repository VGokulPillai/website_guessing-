from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import WordEmbeddingSimilarityIndex  # from gensim 3.7.x (was gensim.models before)
from gensim.similarities import SparseTermSimilarityMatrix
from gensim.similarities import SoftCosineSimilarity
from google_trans_new import google_translator

from src.preprocessing import preprocess_text, lemmatize
from src.ranking import SortingSuppliers
from src.companydata_enricher import *
from src.discoverer import *
from src.scraper import *
from src.searchengine_scraper import *

import os
import warnings
from pprint import pprint
from tqdm import tqdm
import gensim.downloader as api
import json
import numpy as np
import pandas as pd
import xlsxwriter

import nltk
nltk.download('wordnet')
nltk.download('omw-1.4')

warnings.filterwarnings("ignore", category=RuntimeWarning)  # to ignore gensim warning about zero-value corpus norm


def list_splitter(lis, number_items):
    return [lis[i * number_items:(i + 1) * number_items] for i in range((len(lis) + number_items - 1) // number_items)]


class SupplierIdentifier:
    def __init__(self, product, countries_relevant, verbose=False):
        self.collection_to_scrape = client['supplier_scraping_V2']['suppliers_to_scrape_V2']
        self.collection_scraping = client['supplier_scraping_V2']['def_supplier_database_V2']
        self.records_filtered = None
        self.records_filtered_list = []
        self.se_scraper_yahoo = Yahoo()
        self.discoverer = Discoverer()
        self.data_enrichment = DataEnrichment()
        self.scraper = Scraper(collection_to_scrape=None, collection_scraping=self.collection_scraping)
        glove = api.load("glove-wiki-gigaword-50")
        self.similarity_index = WordEmbeddingSimilarityIndex(glove)
        self.classifications_folder = '../assets/industry_codes'
        self.classifications_to_consider = ['Kompass', 'SIC_US_1987_8d']
        # No need to explore NAICS_US_2017_4d classification for d&b data since SIC_US_1987_8d is provided when
        # NAICS_US_2017_4d is also provided
        # TODO: Make classifications dependent on countries_relevant instead of hard coded.
        self.codes_relevant = {}
        self.countries_relevant = countries_relevant
        self.keywords_relevant = []
        self.searchterm = ''
        self.country_language_equivalent = json.load(open('./../assets/country_languagecode_lookup.json', 'r'))
        self.product = None
        self.request_name = None
        self.set_product(product)
        self.results_path = '../results/suppliers'
        os.makedirs(self.results_path, exist_ok=True)
        self.sorting_shortlist = SortingSuppliers(verbose=verbose)
        self.verbose = verbose
        self.max_pages = None

    def set_product(self, product):
        self.product = product
        self.request_name = f'{self.product} in {", ".join(self.countries_relevant)}'

    def identify_relevant_codes(self, min_similarity=0.5, min_codes=1, max_codes=10):
        product = self.product
        query = preprocess_text(product)
        if self.verbose:
            print('Identified industry codes:')
        for classification in self.classifications_to_consider:
            excel_path = os.path.join(self.classifications_folder, classification + '.xlsx')
            df = pd.read_excel(excel_path, dtype={'code': str, 'label': str, 'relevant': int})
            df = df[df['relevant'] == 1]
            df.index = range(0, len(df))
            corpus = [preprocess_text(label) for label in list(df['label'])]
            dictionary = Dictionary(corpus + [query])
            tfidf = TfidfModel(dictionary=dictionary)
            similarity_matrix = SparseTermSimilarityMatrix(self.similarity_index, dictionary, tfidf)
            query_tf = tfidf[dictionary.doc2bow(query)]
            index = SoftCosineSimilarity(tfidf[[dictionary.doc2bow(document) for document in corpus]],
                                         similarity_matrix)
            doc_similarity_scores = index[query_tf]
            sorted_indexes = np.argsort(doc_similarity_scores)[::-1]
            data = []
            [data.append([doc_similarity_scores[idx], df.code[idx], df.label[idx]])
             for rank, idx in enumerate(sorted_indexes) if rank < min_codes or doc_similarity_scores[idx] > min_similarity]
            self.codes_relevant[classification] = pd.DataFrame(data, columns=['score', 'code', 'label'])
            self.codes_relevant[classification] = self.codes_relevant[classification].head(max_codes)
            if self.verbose:
                print(f'\n{classification}:')
                pprint(self.codes_relevant[classification])

    def identify_relevant_keywords(self):
        keywords = self.product
        self.keywords_relevant = preprocess_text(keywords, b_lemmatize=True)
        # TODO: add also the non-lemmatized versions ?
        if self.verbose:
            print(f'\nLooking for these terms in supplier keywords and descriptions: {self.keywords_relevant}')

    def filter_database(self):
        if self.verbose:
            print('\nFiltering the database of suppliers...')
        filtered = {}
        or_filter = {}
        # Filtering for countries
        if self.countries_relevant:
            filtered['postalAddress.addressCountry'] = {"$in": self.countries_relevant}
        # Filtering for codes
        for code in self.codes_relevant:
            if code == 'Kompass':
                or_filter['classifications.Kompass.code'] = {"$in": self.codes_relevant['Kompass']['code'].to_list()}
            else:
                or_filter['classifications.' + code] = {"$in": self.codes_relevant[code]['code'].to_list()}
        if self.keywords_relevant:
            or_filter['$or'] = []
        [or_filter['$or'].append({'description': {"$regex": k, '$options': 'i'}}) for k in self.keywords_relevant]
        [or_filter['$or'].append({'keywords': {"$regex": k, '$options': 'i'}}) for k in self.keywords_relevant]
        if len(or_filter) > 0:
            filtered['$or'] = [{key: value} for key, value in or_filter.items()]
        filtered['website'] = {'$ne': ''}
        self.records_filtered = self.collection_scraping.find(filtered,
                                                              {'name', 'keywords', 'description',
                                                               'classifications', 'postalAddress.addressCountry',
                                                               'website', 'contactEmails'})
        records_filtered_list = list(self.records_filtered)
        if self.verbose:
            print(f'Number of filtered suppliers: {len(records_filtered_list)}')

        return records_filtered_list

    def prepare_searchterms(self, translate=True, add_standard_languages=False):
        searchterms = self.product
        searchterms = preprocess_text(searchterms)
        searchterms_lemmatized = lemmatize(searchterms)
        translator = google_translator()
        searchterms_translated_stored = {}
        translations_unsuccessful = {}
        language_codes = []
        searchterms_list = []
        for i, term in enumerate(searchterms):
            if searchterms_lemmatized[i] == term:
                searchterms_list.append([term])
            else:
                searchterms_list.append([term, searchterms_lemmatized[i]])

        for record in self.records_filtered_list:
            codes = [d['languageCode'].split(',') for d in self.country_language_equivalent if
                                   d['countryName'] == record['postalAddress']['addressCountry']]
            if len(codes) > 0:
                language_codes.extend(codes[0])
        language_codes = list(set(language_codes.extend(['en', 'fr', 'es', 'de']))) if add_standard_languages \
            else list(set(language_codes))

        if translate:
            for idx1, term1 in enumerate(searchterms_list):
                translations_to_add = []
                for idx2, term2 in enumerate(term1):
                    for language_code in language_codes:
                        try:
                            if (language_code in translations_unsuccessful) and \
                                    (term2 in translations_unsuccessful[language_code]):
                                break
                            if language_code in searchterms_translated_stored:
                                translation = searchterms_translated_stored[language_code][idx1][idx2]
                            else:
                                translation = translator.translate(term2, lang_tgt=language_code)
                            translations_to_add.append(translation[0].strip() if isinstance(translation, list)
                                                       else translation.strip())
                        except Exception as E:
                            print('Something went wrong in the translation in code ' + language_code)
                            translations_unsuccessful[language_code] = term2
                searchterms_list[idx1].extend(translations_to_add)
        self.searchterm = '(' + ') AND ('.join([' OR '.join(st2) for st2 in searchterms_list]) + ')'

    def start_search(self):
        websites = []
        records_unique = []
        for record in self.records_filtered_list:
            if record['website'] not in websites:
                websites.append(record['website'])
                records_unique.append(record)
        if self.verbose:
            print(f'Number of unique supplier websites: {len(records_unique)}\n')
            print('Searching with Yahoo for filtered suppliers whose website contains the product terms...')
            print(f'Searching term: {self.searchterm}\n')

        number_items = 8
        while number_items >= 1:
            if self.verbose:
                print(str(len(records_unique)) + ' unique websites are scanned')
            list_splitted = list_splitter(records_unique, int(number_items))
            pa = Parallelizer(self.search_function, input_list=list_splitted, additional_arguments=(),
                              max_workers=500)
            pa.launch_parallelizer()
            records_unique = []
            [records_unique.extend(res[3]) for res in pa.results if res[1]]
            number_items = number_items / 2

        results_to_check = []
        [results_to_check.append(res[0:3]) for res in pa.results if res[0]]

        if self.verbose:
            print(f'Number of remaining supplier candidates in the shortlist: {len(results_to_check)}')

        results_short = []
        for res in results_to_check:
            temp = {}
            temp['mongo_id'] = str(res[0]['_id'])
            temp['supplier_name'] = res[0]['name']
            temp['website'] = res[0]['website']
            temp['subDomains'] = res[1]
            temp['website'] = temp['website'].replace('http://', '')
            temp['website'] = temp['website'].replace('https://', '')
            if temp['subDomains'][0].startswith('http://'):
                temp['website'] = 'http://' + temp['website']
            elif temp['subDomains'][0].startswith('https://'):
                temp['website'] = 'https://' + temp['website']
            temp['country'] = res[0]['postalAddress']['addressCountry']
            temp['email'] = ''
            if 'contactEmails' in res[0]:
                if isinstance(res[0]['contactEmails'], list) and len(res[0]['contactEmails']) > 0:
                    temp['email'] = res[0]['contactEmails'][0]['email']
                else:
                    pass  # TODO: Implement here the extraction of email contacts and also update the supplier DB

            results_short.append(temp)

        with open(os.path.join(self.results_path, f'{self.request_name}.json'), 'w') as outfile:
            json.dump(results_short, outfile, indent=4)

        workbook = xlsxwriter.Workbook(os.path.join(self.results_path, f'{self.request_name}.xlsx'))
        worksheet = workbook.add_worksheet()
        for row_num, res in enumerate(results_to_check):
            cnt = 0
            worksheet.write(row_num, cnt, str(res[0]['_id']))
            cnt += 1
            worksheet.write_url(row_num, cnt, res[0]['website'])
            cnt += 1
            worksheet.write(row_num, cnt, res[0]['description'])
            cnt += 1
            worksheet.write(row_num, cnt, res[2])
            for idx, w in enumerate(res[1]):
                cnt += 1
                worksheet.write_url(row_num, cnt, w, string='[' + str(idx) + ']')
        workbook.close()

        return results_short

    def search_function(self, list_splitted):
        links_found, results_total = self.se_scraper_yahoo.retrieve_results_yahoo(self.searchterm,
                                                                                  in_site=[record['website']
                                                                                           for record in list_splitted],
                                                                                  typ='single', max_recall=3)
        if links_found:
            return [list_splitted[0], links_found, results_total, list_splitted]
        return [None, None, None]

    def analyze_industry_codes_identification(self, products=None):
        if products is None:
            products = ['Pea Protein', 'Date Paste', 'Lecithin', 'Coconut Sugar', 'Maple Syrup', 'Almonds',
                        'Citric Acid', 'Tapioca']
        self.classifications_to_consider = ['ISIC_World_2008_4d', 'NACE_EU_2008_4d', 'Kompass', 'SIC_GB_2007_5d']
        results = {}
        self.verbose = True
        for product in products:
            print(f'\nProduct: {product}')
            results[product] = {}
            self.set_product(product)
            self.identify_relevant_codes(min_similarity=0.5, max_codes=5)
            for classification in self.codes_relevant:
                results[product][classification] = []
                for i in range(len(self.codes_relevant[classification])):
                    results[product][classification].append(self.codes_relevant[classification].iloc[i].to_dict())

        os.makedirs('../results/stats/', exist_ok=True)
        with open('../results/stats/industry_codes_identification.json', 'w') as f:
            json.dump(results, f)

    def get_data_from_supplier_websites(self, suppliers, save_path=None, max_pages=7):
        print('\nScraping the website data for the suppliers in the shortlist... ')
        # TODO: multiprocess the scrapping to get the results faster
        self.max_pages = max_pages

        pa = Parallelizer(self.get_data_for_one_supplier, input_list=suppliers, additional_arguments=(),
                          max_workers=500)
        pa.launch_parallelizer()
        suppliers = [res[0] for res in pa.results]
        supplier_without_text = sum([res[1] for res in pa.results])

        print(f'\nSupplier without text found on their website: {supplier_without_text}')

        if save_path:
            with open(save_path, 'w') as f_w:
                json.dump(suppliers, f_w)

        return suppliers

    def get_data_for_one_supplier(self, supplier):
        supplier['text'] = []
        pages = supplier['subDomains']
        if len(pages) > self.max_pages > 0:
            random.shuffle(pages)
            pages = pages[:self.max_pages]
            print(f'For {supplier["website"]} domain, the number of pages has been limited to {self.max_pages}')
        for page in pages:
            content = self.scraper.scrape_website(page)
            page_text, _ = self.scraper.get_data_from_scraping(content)
            if page_text:
                supplier['text'].extend(page_text)

        if len(supplier['text']) == 0:
            print(f'No text is available for the supplier {supplier["supplier_name"]}')
            supplier_without_text = 1
        else:
            supplier_without_text = 0

        return supplier, supplier_without_text

    def sort_supplier_shortlist(self, shortlist):
        if len(shortlist) == 0:
            raise Exception('You need at least one supplier candidate to start the ranking process !')
        self.sorting_shortlist.build_documents_representations(shortlist)
        self.sorting_shortlist.build_similarity_index()
        self.sorting_shortlist.get_relevant_suppliers(self.product, self.request_name, shortlist)


def extract_products(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)

    products = set(item['Ingredients'] for item in data)
    products = sorted(products)
    print(f'\nNumber of extracted products: {len(products)}')

    return products


if __name__ == "__main__":
    product = 'organic almonds'
    countries_relevant = ['United Kingdom']

    si = SupplierIdentifier(product, countries_relevant, verbose=True)

    # products = extract_products('../json_data/target_products.json')
    # si.analyze_industry_codes_identification(products)

    shortlist_path = os.path.join(si.results_path, f'{si.request_name}.json')
    if os.path.exists(shortlist_path):
        print('\nLoading the website data for suppliers in the shortlist...')
        with open(shortlist_path) as f:
            shortlist = json.load(f)
    else:
        # Filter the supplier database
        si.identify_relevant_codes(min_similarity=0.5, max_codes=5)
        si.identify_relevant_keywords()
        si.records_filtered_list = si.filter_database()

        # Search with Yahoo for suppliers whose website contains the product terms
        si.prepare_searchterms(translate=False, add_standard_languages=False)
        shortlist = si.start_search()

        shortlist = si.get_data_from_supplier_websites(shortlist, shortlist_path)

    # Sort the supplier shortlist according to their relevance to the product terms
    si.sort_supplier_shortlist(shortlist)
