from re import sub
from gensim.utils import simple_preprocess
from typing import List
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import download


download('stopwords')  # Download stopwords list
stopwords = stopwords.words()
lem = WordNetLemmatizer()


def preprocess_text(text: str, b_lemmatize: bool = False) -> List[str]:
    # Clean input string
    text = sub(r'<img[^<>]+(>|$)', " image_token ", text)
    text = sub(r'<[^<>]+(>|$)', " ", text)
    text = sub(r'\[img_assist[^]]*?\]', " ", text)
    text = sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', " url_token ",
               text)
    tokens = [token for token in simple_preprocess(text, min_len=0, max_len=float("inf"))
              if token not in stopwords and token.isalpha()]
    if b_lemmatize:
        tokens = lemmatize(tokens)
    return tokens


def lemmatize(tokens: List[str]) -> List[str]:
    return [lem.lemmatize(token) for token in tokens]
