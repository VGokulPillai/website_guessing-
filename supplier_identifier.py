from google_trans_new import google_translator
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem import PorterStemmer
from src.companydata_enricher import *
from src.discoverer import *
from src.mongoDB import *
from src.scraper import *
from src.searchengine_scraper import *
from urllib.parse import quote
from wordhoard import Hypernyms
from wordhoard import Synonyms

import copy
import json
import pandas as pd
import xlsxwriter


class SupplierIdentifier:
    def __init__(self):
        self.collection_to_scrape = client['supplier_scraping_V2']['suppliers_to_scrape_V2']
        self.collection_scraping = client['supplier_scraping_V2']['supplier_database_V2']
        self.records_filtered = []
        self.se_scraper_yahoo = Yahoo()
        self.discoverer = Discoverer()
        self.scraper = Scraper(collection_to_scrape=None, collection_scraping=self.collection_scraping)
        self.kompass_classification = pd.read_excel('./site_specific/kompass/kompassCodes.xlsx', dtype=str)
        self.kompass_classifications_relevant = pd.DataFrame(columns=self.kompass_classification.columns)
        self.codes_relevant = {}
        self.countries_relevant = []
        self.keywords_relevant = []
        self.data_enrichment = DataEnrichment()
        self.country_languagecode_lookup_file = './../assets/country_languagecode_lookup.json'
        self.country_language_equivalent = json.load(open(self.country_languagecode_lookup_file, 'r'))

    def identify_relevant_kompass_codes(self, keyword, codes_add=[], include_parentcode=False,
                                        include_keyword_in_codes=False, include_keyword_stemmed_in_codes=False,
                                        include_hypernym=False):
        codes_add.extend([code for code in codes_add])
        if include_parentcode:
            codes_add.extend([code[0:5] for code in codes_add if len(code) == 7])
        df_codes = self.kompass_classification[self.kompass_classification['code'].isin(codes_add)]
        self.kompass_classifications_relevant = pd.concat([self.kompass_classifications_relevant, df_codes])
        # Kompass codes containing the word
        if include_keyword_in_codes:
            df_keyword = self.kompass_classification[self.kompass_classification['label'].str.contains(keyword)]
            self.kompass_classifications_relevant = pd.concat([self.kompass_classifications_relevant, df_keyword])
        # Kompass codes containing stemmed word
        if include_keyword_stemmed_in_codes:
            porter = PorterStemmer()
            keyword_stemmed = porter.stem(keyword)
            df_keyword_stemmed = \
                self.kompass_classification[self.kompass_classification['label'].str.contains(keyword_stemmed)]
            self.kompass_classifications_relevant = pd.concat(
                [self.kompass_classifications_relevant, df_keyword_stemmed])
        # Kompass codes containing hypernym
        if include_hypernym:
            hypernyms = Hypernyms(keyword_stemmed).find_hypernyms()
            print('Not implemented yet!')
            # TODO: Add synonyms
        self.kompass_classifications_relevant = self.kompass_classifications_relevant.drop_duplicates()
        self.codes_relevant['Kompass'] = list(self.kompass_classifications_relevant['code'])

    def filter_keywords(self, keywords, include_directories=None):
        self.keywords_relevant = keywords if isinstance(keywords, list) else [keywords]
        if include_directories:
            for directory in include_directories:
                if directory == 'Europages':
                    for keyword in keywords:
                        if len(self.countries_relevant) > 0:
                            for country in self.countries_relevant:
                                url = 'https://www.europages.co.uk/companies/' + quote(
                                    country) + '/' + keyword + '.html'
                                self.discoverer.launch_europages(url)
                        else:
                            url = 'https://www.europages.co.uk/companies/' + keyword + '.html'
                            self.discoverer.launch_europages(url)
                else:
                    print('Directory not implemented!')
            self.discoverer.launch_discoverer(push_to_database=False)
            docs_not_in_database = [doc for doc in self.discoverer.documents_to_discover
                                    if self.scraper.db.collection_scraping.count_documents(
                    {'_id': doc['url']}, limit=1) == 0]

            self.scraper.documents_to_scrape = docs_not_in_database
            self.scraper.launch_scraper(update_to_scrape=False)

    def filter_codes(self, codes):
        self.codes_relevant = codes

    def filter_database(self):
        filtered = {}
        or_filter = {}
        # Filtering for locations
        if len(si.countries_relevant) > 0:
            filtered['postalAddress.addressCountry'] = \
                {"$in": list(si.countries_relevant) if isinstance(si.countries_relevant,
                                                                  str) else si.countries_relevant}
        # Filtering for classifications
        for code in self.codes_relevant:
            if code == 'Kompass':
                or_filter['classifications.Kompass.code'] = {"$in": self.codes_relevant['Kompass']}
            else:
                or_filter['classifications.' + code] = {"$in": self.codes_relevant[code]}
        # if len(self.keywords_relevant) == 1:
        #     or_filter['description'] = {"$regex": self.keywords_relevant[0], '$options': 'i'}
        # elif len(self.keywords_relevant) > 1:
        or_filter['$or'] = []
        for keyword in self.keywords_relevant:
            or_filter['$or'].append({'description': {"$regex": keyword, '$options': 'i'}})
            or_filter['$or'].append({'keywords': {"$regex": keyword, '$options': 'i'}})
        if len(or_filter) > 0:
            filtered['$or'] = [{key: value} for key, value in or_filter.items()]
        self.records_filtered = list(self.supplier_database.find(filtered).limit(100000))
        print(str(len(self.records_filtered)) + ' longlisted')

    def guess_websites_launch_function(self):
        records_relevant = [record for record in self.records_filtered if (record['website'] is '')
                            and ('companydataEnriched' not in record or len(record['companydataEnriched']) == 0)]
        pa = Parallelizer(self.guess_websites, input_list=records_relevant, additional_arguments=(),
                          max_workers=10)
        print('Launching Website guessing for :' + str(records_relevant) + ' records')
        pa.launch_parallelizer()
        for record, res in zip(records_relevant, pa.results):
            if 'companydataEnriched' in record:
                record['companydataEnriched'].extend(res)
            else:
                record['companydataEnriched'] = res
            self.supplier_database.update_one({'_id': record['_id']},
                                              {'$set': {'companydataEnriched': record['companydataEnriched']}})

    def guess_websites(self, record):
        return self.data_enrichment.website_from_search_engine(company_name=record['name'],
                                                               country=record['postalAddress']['addressCountry'])

    def list_splitter(self, lis, number_items):
        n = max(1, number_items)
        return [lis[i * number_items:(i + 1) * number_items] for i in
                range((len(lis) + number_items - 1) // number_items)]

    def start_search(self, searchterms, include_websites=['Directories', 'Yahoo']):
        records_list = []
        if not isinstance(searchterms, list):
            searchterms = [searchterms]
        searchterms_original = searchterms
        searchterms_translated_stored = {}
        translator = google_translator()
        Lem = WordNetLemmatizer()
        searchterms_original_stemmed = [Lem.lemmatize(term) for term in searchterms_original]
        if searchterms_original_stemmed != searchterms_original:
            searchterms_original_s_us = \
                [j for i in zip(searchterms_original, searchterms_original_stemmed) for j in i]
            searchterms_original_s_us = \
                [searchterms_original_s_us[i:i + 2]
                 for i in range(0, len(searchterms_original_s_us), 2)]
        else:
            searchterms_original_s_us = [searchterms_original]
        searchterms_copy = copy.deepcopy(searchterms_original_s_us)

        translations_unsuccessful = {}
        language_codes = []
        for record in self.records_filtered:
            searchterms_original_s_us = copy.deepcopy(searchterms_copy)
            # Translating searchterms
            language_codes.extend([d['languageCode'].split(',') for d in self.country_language_equivalent if
                              d['countryName'] == record['postalAddress']['addressCountry']][0])
        language_codes = list(set(language_codes))
        if len(language_codes) > 0:
            for language_code in language_codes:
                try:
                    temp = copy.deepcopy(searchterms_copy)
                    for idx1, term1 in enumerate(searchterms_copy):
                        for idx2, term2 in enumerate(term1):
                            if (language_code in translations_unsuccessful) and \
                                    (term2 in translations_unsuccessful[language_code]):
                                break
                            if language_code in searchterms_translated_stored:
                                translation = searchterms_translated_stored[language_code][idx1][idx2]
                            else:
                                translation = translator.translate(term2, lang_tgt=language_code)
                            if isinstance(translation, list):
                                searchterms_original_s_us[idx1].append(translation[0].strip())
                                temp[idx1][idx2] = translation[0].strip()
                            else:
                                searchterms_original_s_us[idx1].append(translation.strip())
                                temp[idx1][idx2] = translation.strip()
                    if language_code not in searchterms_translated_stored:
                        searchterms_translated_stored[language_code] = temp
                    searchterm = ' AND '.join(['(' + ' OR '.join(term) + ')' for term in searchterms_original_s_us])
                except Exception as E:
                    print('Something went wrong in the translation in code ' + language_code)
                    translations_unsuccessful[language_code] = term2
            searchterm = '(' + searchterm + ')'

        for record in self.records_filtered:
            if record['website'] is not '':
                records_list.append([record, record['website'], searchterm])
            # elif 'companydataEnriched' in record and len(record['companydataEnriched']) > 0:
            #     records_list.append([record, record['companydataEnriched'][0]['url'], searchterm])
            else:
                pass

        records_unique = []
        for record_u in records_list:
            if (record_u[1:2] not in [sublist[1:2] for sublist in records_unique]) and (record_u[1] is not None):
                records_unique.append(record_u)

        number_items = 16
        while number_items >= 1:
            list_splitted = self.list_splitter(records_unique, int(number_items))
            # TODO: Make sure only the same searchterms are split (differentiation by country!)
            pa = Parallelizer(self.search_function, input_list=list_splitted, additional_arguments=(),
                              max_workers=500)
            pa.launch_parallelizer()
            records_unique = []
            [records_unique.extend(res[3]) for res in pa.results if res[1]]
            number_items = number_items / 2

        results_to_check = []
        [results_to_check.append(res[0:3]) for res in pa.results if res[0]]
        print(str(len(self.records_filtered)) + ' suppliers shortlisted')
        # # Getting email addresses
        # websites_for_emails = [res[0]['website'] for res in results_to_check]
        # emails = self.data_enrichment.email_from_website_launcher(websites_for_emails)
        # [self.data_enrichment.email_from_website_launcher(res[0]['website']) for res in results_to_check]


        # df = pd.DataFrame()
        # df['id'] = [r[0]['_id'] for r in results_to_check]
        # df['name'] = [r[0]['name'] for r in results_to_check]
        # df['description'] = [r[0]['description'] for r in results_to_check]
        # df['website'] = [r[0]['website'] for r in results_to_check]
        # df['keywords'] = [r[0]['keywords'] for r in results_to_check]
        #
        # df['websites_found'] = [r[1] for r in results_to_check]
        # df['contactEmails'] = [r[0]['contactEmails'] if 'contactEmails' in r else [] for r in results_to_check]
        # df['relevant'] = [r[1] for r in results_to_check]

        workbook = xlsxwriter.Workbook('C:\Daten\Miscellaneous\contacts.xlsx')
        worksheet = workbook.add_worksheet()
        for row_num, res in enumerate(results_to_check):
            cnt = 0
            worksheet.write(row_num, cnt, res[0]['_id'])
            cnt += 1
            worksheet.write(row_num, cnt, res[0]['name'])
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
        return results_to_check

    def search_function(self, list_splitted):
        links_found, results_total = self.se_scraper_yahoo.retrieve_results_yahoo(list_splitted[0][2],
                                                                                  in_site=[record[1] for record in
                                                                                           list_splitted],
                                                                                  typ='single', max_recall=4)
        if links_found:
            return [list_splitted[0][0], links_found, results_total, list_splitted]
        return [None, None, None]


if __name__ == "__main__":
    si = SupplierIdentifier()
    si.countries_relevant = ['France', 'Italy', 'Spain']
    # si.identify_relevant_kompass_codes(keyword="almond", codes_add=['0252001', '0386015', '8125012'],
    #                                    include_parentcode=False,
    #                                    include_keyword_in_codes=False,
    #                                    include_keyword_stemmed_in_codes=False,
    #                                    include_hypernym=False)
    si.filter_codes({'Kompass': ['0252001', '0386015', '02520', '0514036']})
    si.filter_keywords(['almond'], include_directories=['Europages'])
    si.filter_database()
    #si.guess_websites_launch_function()
    si.start_search(searchterms=['almonds'])

    print(122)
