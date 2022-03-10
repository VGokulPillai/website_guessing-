import json

from src.preprocessing import preprocess_text

from gensim.corpora import Dictionary
from gensim.models import TfidfModel, KeyedVectors
from gensim.similarities import WordEmbeddingSimilarityIndex  # from gensim 3.7.x (was gensim.models before)
from gensim.similarities import SparseTermSimilarityMatrix, MatrixSimilarity
from gensim.similarities import SoftCosineSimilarity
import gensim.downloader as api

import os
from typing import List

import nltk
nltk.download('wordnet')
nltk.download('omw-1.4')


class SortingSuppliers:
    def __init__(self, word_embeddings_model_name=None, b_tfidf=True, verbose=False):
        # TODO: Experiment with various parameters values for word_embeddings_model_name and b_tfidf once we get a
        # validation/test dataset
        self.word_embeddings_model_name = word_embeddings_model_name
        self.b_tfidf = b_tfidf
        self.verbose = verbose

        if word_embeddings_model_name:
            # Retrieve the pre-trained word embeddings model
            os.makedirs('models', exist_ok=True)
            emb_model_path = os.path.join('models', self.word_embeddings_model_name)
            try:
                self.emb_model = KeyedVectors.load_word2vec_format(emb_model_path)
            except FileNotFoundError:
                self.emb_model = api.load(self.word_embeddings_model_name)
                self.emb_model.save_word2vec_format(emb_model_path)

        self.dictionary = None
        self.tfidf = None
        self.docs_repr = None
        self.similarity_index = None
        self.results_path = '../results/relevant_suppliers'
        os.makedirs(self.results_path, exist_ok=True)

    def build_documents_representations(self, suppliers):
        docs = []
        for supplier in suppliers:
            docs.append(preprocess_text(' '.join(supplier['text'])))

        self.dictionary = Dictionary(docs)
        # dictionary.filter_extremes(keep_n=1000)  # Keep only the n most frequent terms in the dictionary
        print(f"Length of the dictionary: {len(self.dictionary)}")

        docs_repr = [self.dictionary.doc2bow(doc) for doc in docs]

        if self.b_tfidf:
            self.tfidf = TfidfModel(docs_repr)
            docs_repr = [self.tfidf[doc] for doc in docs_repr]

        self.docs_repr = docs_repr

    def build_similarity_index(self):
        if self.word_embeddings_model_name is None:
            self.similarity_index = MatrixSimilarity(self.docs_repr, num_features=len(self.dictionary))

        else:
            embeddings_index = WordEmbeddingSimilarityIndex(self.emb_model)
            if self.verbose:
                print('Computing the term similarity matrix...')
            termsim_matrix = SparseTermSimilarityMatrix(
                embeddings_index,
                self.dictionary,
                self.tfidf if self.b_tfidf else None
            )
            self.similarity_index = SoftCosineSimilarity(self.docs_repr, termsim_matrix)

    def get_relevant_suppliers(self, product: str, request_name: str, suppliers: List):
        query = preprocess_text(product)
        query_repr = self.dictionary.doc2bow(query)
        if self.b_tfidf:
            query_repr = self.tfidf[query_repr]

        # Compute the similarity of the query with each document
        sims = self.similarity_index[query_repr]
        for i, supplier in enumerate(suppliers):
            if len(supplier['text']) == 0:  # Set negative scores if we had not retrieved the text of a supplier
                supplier['relevance'] = '-1'
            else:
                supplier['relevance'] = f'{sims[i]:.3f}'
            # These two fields won't be needed to later outreach suppliers
            del supplier['text']
            del supplier['subDomains']

        # Sort the similarity scores by decreasing order
        suppliers.sort(key=lambda x: x['relevance'], reverse=True)

        # Save the similarity scores in the results folder
        with open(os.path.join(self.results_path, f'{request_name}.json'), 'w') as f:
            json.dump(suppliers, f)

        if self.verbose:
            print(f'Similarity scores for the product request "{request_name}" '
                  f'have been saved on the results folder')
