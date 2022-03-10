import json
import os

from pymongo import MongoClient
import ssl
import pandas as pd
from pprint import pprint

ssl._create_default_https_context = ssl._create_unverified_context

with open('../credentials/mongoDB.txt', 'r') as f:
    username = f.readline().strip()
    pwd = f.readline().strip()
client = MongoClient(f'mongodb+srv://{username}:{pwd}'
                     f'@cluster0.hdjhe.mongodb.net/supplier_scraping_V2?retryWrites=true&w=majority')


def get_supplier_statistics(countries=[], analyze_origins=False, analyze_codes=False, only_relevant_codes=True,
                            analyze_countries=False):
    db = client['supplier_scraping_V2']['def_supplier_database_V2']
    nb_suppliers = db.count_documents({})
    print(f'Total number of suppliers {nb_suppliers}')

    filtered = {}
    if countries:
        filtered['postalAddress.addressCountry'] = {"$in": countries}
        # print(f'List of all supplier countries: {db.distinct("postalAddress.addressCountry")}')
        print(f'Countries: {countries}')
        nb_suppliers = db.count_documents(filtered)
        print(f'Number of suppliers in these countries: {nb_suppliers}')

    files_path = '../assets/industry_codes'
    files = os.listdir(files_path)
    classifications = [os.path.splitext(file)[0] for file in files]

    if analyze_origins:
        origins = db.distinct('origin', filtered)
        print(f'\nOrigins of the suppliers: {origins}')
        for origin in origins:
            ori_filtered = {**filtered, **{"origin": origin}}
            all_count = db.count_documents(ori_filtered)
            with_classifications = db.count_documents({**ori_filtered, **{'classifications': {'$nin': [None, '', {}, []]}}})
            print(f'{origin}: {all_count}, {with_classifications} with non-empty classifications')

            if with_classifications > 0:
                for classification in classifications:
                    new_filtered = {**ori_filtered,
                                    **{f'classifications.{classification}': {'$nin': [None, '', {}, []]}},
                                    }
                    print(f'Non-empty values for {classification}: {db.count_documents(new_filtered)}')
            print('\n')

    if analyze_codes:
        industry_codes = {classification: pd.read_excel(os.path.join(files_path, classification + '.xlsx'), dtype=str)
                          for classification in classifications}
        print('Most frequent industry codes for each classification')
        for classification in classifications:
            print(f'\nClassification: {classification}')
            key = f"classifications.{classification}"
            pipeline = []
            if classification == 'Kompass':
                key = key + '.code'
                pipeline.append({"$unwind": "$classifications.Kompass"})
                # see https://stackoverflow.com/questions/51956436/mongodb-nested-array-aggregation

            pipeline.append({"$match": {**filtered, key: {"$nin": [None, '', {}, []]}}})
            pipeline.append({"$group": {"_id": f"${key}", "count": {"$sum": 1}}})
            pipeline.append({"$sort": {"count": -1}})
            counter = db.aggregate(pipeline)

            i = 0
            for item in counter:
                code_str = item['_id'][0] if isinstance(item['_id'], list) else item['_id']
                count = item['count']
                code = industry_codes[classification].loc[industry_codes[classification]['code'] == code_str]
                if not len(code):
                    print(f'Code {code_str} was not found')
                else:
                    assert (len(code) == 1)
                    code = code.iloc[0]
                    if only_relevant_codes and not int(code['relevant']):
                        continue
                    label = code['label']
                    print(f'{label} ({code_str}): {count} out of {nb_suppliers} suppliers')

                i += 1
                if i >= 10:
                    break

    if analyze_countries:
        pipeline = [
            {"$group": {"_id": "$postalAddress.addressCountry", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        counter = db.aggregate(pipeline)
        for item in counter:
            print(f'Number of suppliers in {item["_id"]}: {item["count"]}')

    return filtered


def extract_random_records(db, nb_records=10, save_path=None, filtered=None):
    if filtered is None:
        pipeline = []
    else:
        pipeline = [filtered]
    pipeline.append({'$sample': {'size': nb_records}})
    records = list(db.aggregate(pipeline))

    if save_path:
        with open(save_path, 'w') as f:
            json.dump(records, f, default=str)

    return records


if __name__ == '__main__':
    db = client['supplier_scraping_V2']['def_supplier_database_V2']

    # nb_records = int(1e4)
    # records = extract_random_records(db, nb_records, save_path=f'../json_data/DB_records/{nb_records}_records.json')

    countries = ['United Kingdom']
    filtered = get_supplier_statistics(countries,
                                       analyze_origins=False,
                                       analyze_codes=False, only_relevant_codes=True,
                                       analyze_countries=True)
