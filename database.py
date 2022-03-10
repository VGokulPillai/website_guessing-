from datetime import datetime
from src.companydata_enricher import Email, Website
from src.mongoDB import client

import string


class Database:
    def __init__(self, collection_to_scrape=None, collection_scraping=None):
        if collection_to_scrape is not None:
            self.collection_to_scrape = collection_to_scrape
        if collection_scraping is not None:
            self.collection_scraping = collection_scraping

    def insert_to_scrape(self, data_many):
        try:
            self.collection_to_scrape.insert_many(data_many, ordered=False)
        except Exception as E:
            print(E)

    def insert_document(self, data, update_to_scrape):
        if bool(data) is True:  # Got data
            if update_to_scrape:
                self.collection_to_scrape.update_one(
                    {"_id": data['_id']},
                    {"$set": {"discovered": datetime.now().timestamp()}})
            self.collection_scraping.insert_one(data)
        else:  # Failed
            if update_to_scrape:
                self.collection_to_scrape.update_one(
                    {"_id": data['_id']},
                    {"$set": {"discovered": -2}})


class ToScrapeEntry:
    def __init__(self, id='', origin='', url='', discovered=''):
        self.data = {}
        self.data['_id'] = id
        self.data['origin'] = origin
        self.data['url'] = url
        self.data['discovered'] = discovered


class ScrapingEntry:
    def __init__(self, _id='', lastScraped=None, url='', name='', classifications={}, streetAddress='', addressLocality='',
                 addressRegion='', addressCountry='', postalCode='', telephone='', website='', mainActivity='',
                 vatId='', category='', subcategory='', corporateCapital='', yearEstablished=None, legalForm='',
                 locationType='', revenue='', employeesSite='', facilitySize='', products=[], origin='',
                 keywords=[], certifications=[], brands=[],
                 description='', registrationNumber='', logoUrl='', headquarter='', contactEmail=''):
        self.data = {}
        self.data['_id'] = _id
        self.data['lastScraped'] = lastScraped
        self.data['url'] = url
        self.data['name'] = name
        self.data['classifications'] = classifications
        self.data['postalAddress'] = {'streetAddress': string.capwords(streetAddress),
                                      'addressLocality': string.capwords(addressLocality),
                                      'addressRegion': string.capwords(addressRegion),
                                      'addressCountry': string.capwords(addressCountry),
                                      'postalCode': postalCode}
        self.data['telephone'] = telephone
        self.data['website'] = website
        self.data['mainActivity'] = string.capwords(mainActivity)
        self.data['vatId'] = vatId
        self.data['category'] = category
        self.data['subcategory'] = subcategory
        self.data['corporateCapital'] = corporateCapital
        self.data['yearEstablished'] = yearEstablished
        self.data['legalForm'] = legalForm
        self.data['locationType'] = locationType
        self.data['revenue'] = revenue
        self.data['employeesSite'] = employeesSite
        self.data['facilitySize'] = facilitySize
        self.data['products'] = products
        self.data['origin'] = origin
        self.data['keywords'] = [x.lower() for x in keywords]
        self.data['certifications'] = certifications
        self.data['brands'] = brands
        self.data['description'] = description
        self.data['registrationNumber'] = registrationNumber
        self.data['logoUrl'] = logoUrl
        self.data['headquarter'] = headquarter
        self.data['contactEmails'] = [Email(contactEmail, 'Directory', origin).data] \
            if contactEmail != '' and origin != '' else []
        self.data['companydataEnriched'] = [Website(website, origin).data] \
            if website != '' and origin != '' else []


