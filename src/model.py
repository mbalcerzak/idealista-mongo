import pandas as pd
from src.db_mongo import get_db
from collections import Counter
import typing
from collections import Counter, OrderedDict
import re
import requests
import spacy
import json


def tf_idf_vectorizer():
    df = pd.read_parquet("data/raw_merged.parquet")
    desc = df["description"]
    desc_list = []
    for x in desc:
        desc_list += str(''.join([i for i in str(x) if not i.isdigit()])).split()

        print(len(desc_list))
    # print(desc_list)

    c = Counter(desc_list)
    c = OrderedDict(c.most_common())
    


def load_stopwords_es():
    file_path = "data/stopwords_es.txt"
    lines_list = []
    with open(file_path, 'r') as file:
        lines_list = [line.strip() for line in file.readlines()]

    return lines_list


def get_flats_prq() -> typing.List[pd.DataFrame]:
    """Returns flat info and price collection from Mongo database in a form of DataFrame"""
    db = get_db()
    collection_flats = db["_flats"]
    collection_prices = db["_prices"]
 
    flats = collection_flats.find()
    prices = collection_prices.find()

    cols_flats = [
        'propertyCode', 'propertyType',  'size', 'floor', 'exterior', 'rooms', 'district',
        'bathrooms', 'province', 'municipality', 'description', 'status', 'newDevelopment',
        'hasLift', 'numPhotos', 'hasVideo', 'parkingSpace', 'hasPlan', 'has3DTour', 'has360',
        'priceInfo', 'labels'
        ]

    cols_price = ['propertyCode', 'price', 'date']

    df_flats = pd.DataFrame.from_dict(flats)
    df_flats = df_flats[cols_flats]

    df_prices = pd.DataFrame.from_dict(prices)
    df_prices = df_prices[cols_price]

    return df_flats, df_prices


def save_merged_flats_prices() -> None:
    """
    Get prices and flats from two tables and merge by PropertyCode.
    There can be multiple prices per flat
    """
    df_flats, df_prices = get_flats_prq()
    merged_df = pd.merge(df_prices, df_flats, on="propertyCode")
    merged_df.to_parquet("data/raw_merged.parquet")


def get_terrace_from_description(description: str) -> str:
    if description:
        sentences = description.split(". ")

        for sentence in sentences:
            terrace_sentence = find_terrace_sentence(sentence)
            if terrace_sentence:
                return terrace_sentence 
            

def find_terrace_sentence(sentence: str) -> str:
    terrace_sentence = None

    # Define a regex pattern to capture the size of the terrace
    # This pattern looks for the word 'terrace' then captures the number
    pattern = r"\bterraza.*?(\d{1,5})\s*(\bmetros|sqm|sq|m|m2|sq),?"

    # Find part that says e.g. 'terraza de aprox tamano 50 metros"
    terrace_match = re.search(pattern, sentence, re.IGNORECASE)

    if terrace_match:
        terrace_sentence = terrace_match.group()
        # print("-------------")
        # print(sentence)
        # print(f"{terrace_sentence=}")

    return terrace_sentence


def find_terrace_size(terrace_sentence: str) -> str:
    # Find the number which represents the terrace size
    terrace_size = None
    if terrace_sentence:
        pattern_num = re.compile(r'\d{1,5}')
        terrace_size_match = pattern_num.search(terrace_sentence)

        if terrace_size_match:
            terrace_size = terrace_size_match.group()

    return terrace_size 


def get_balcon(description: str) -> bool:
    if "balcón" in description:
        return True  
    return False


def get_legal_status_info(description: str) -> list:
    words_okupas = ["ocupado", "okupado", "desocupación", 
                    "ocupacion", "ocupas", "ocupación", "no se puede visitar",
                    "no se pueden visitar", "no se puede entrar"]
    words_alquilado = ["alquilado", "alquilada", "alquiladas"]
    words_nudo = ["nuda", "nudo"]

    okupas, alquilado, nudo = False, False, False

    for word in words_okupas:
        if word in description:
            okupas = True

    for word in words_alquilado:
        if word in description:
            alquilado = True

    for word in words_nudo:
        if word in description.split(" "):
            nudo = True

    return pd.Series([okupas, alquilado, nudo])
    

def get_parking_info(parkingSpace: str):
    print(parkingSpace)



def clean_description(description: str) -> str:
    if not description: return None

    # Lowercasing: Convert all text to lowercase to maintain uniformity.  
    description = description.lower()
    
    # Remove Punctuation and Special Characters
    description = re.sub(r'[^\w\s]', ' ', description)

    # Tokenization: Split text into words
    description = description.split()

    # # Stop Words Removal: Remove common words that do not carry significant meaning 
    # (e.g., "hay", "a", "mucho", "mis" ...).
    stopwords = load_stopwords_es()
    description_no_stopwords = [x for x in description if x not in stopwords]
    
    # Lemmatization: Reduce words to their base or root form. 
    # Stemming might reduce "running" to "run", while lemmatization ensures "better" becomes "good".
    nlp = spacy.load("es_core_news_sm")
    doc = nlp(" ".join(description_no_stopwords))
    # Extract lemmas
    lemmas = [token.lemma_ for token in doc]

    return " ".join(lemmas)


def features_from_description(description: str):
    # Bag of Words (BoW): Represent text as a collection of word counts.  
    # Term Frequency-Inverse Document Frequency (TF-IDF): Adjust word counts based on their frequency in the entire corpus to highlight important words.  
    # Word Embeddings: Use pre-trained embeddings like Word2Vec, GloVe, or BERT to convert words into dense vectors that capture semantic meaning.  
    # Part-of-Speech (POS) Tagging: Annotate words with their grammatical roles (nouns, verbs, adjectives, etc.).  
    # N-grams: Create features based on contiguous sequences of 'n' words (e.g., bi-grams, tri-grams).  
    # Named Entity Recognition (NER): Identify and classify entities (e.g., people, locations, organizations) in the text.  
    # Sentiment Scores: Add features representing the sentiment polarity of text segments.  
    # Syntactic Dependencies: Analyze and incorporate syntactic relationships between words in sentences. 

    df = pd.read_csv("data/terrace_results.csv", encoding='utf-8')
    df["description_clean"] = df["description"].apply(lambda x: clean_description(x))
    desc = df["description_clean"]
    
    desc_list = [[].append(x) for x in desc]

    print(len(desc_list))



def process_data(df: pd.DataFrame) -> pd.DataFrame:

    df["description"] = df["description"].apply(lambda x: str(x))
    # TODO flatten parkingSpace column

    # df = df.head(100)

    # df["description_clean"] = df["description"].apply(lambda x: clean_description(x))

    # df["terrace_sentence"] = df["description"].apply(lambda x: get_terrace_from_description(x))
    # df["terraceSize"] = df["terrace_sentence"].apply(lambda x: find_terrace_size(x))

    # df[["okupas", "alquilado", "nudo"]] = df["description"].apply(lambda x: get_legal_status_info(x))
    # df["balcon"] = df["description"].apply(lambda x: get_balcon(x))

    df["parking"] = get_parking_info(df["description"])
  
    df.to_csv("output/okupas_processed.csv")
    
    # print(df[["propertyCode", "terrace_sentence", "terraceSize"]].head(20))

    # c = Counter(df["propertyCode"])
    # counter = pd.DataFrame.from_dict(c, orient='index').reset_index()
    # flats_price_changed = counter.rename(columns={"index":"propertyCode", 0: "priceChangeCount"})

    # df = pd.merge(df, flats_price_changed, on="propertyCode")
    # df["priceArea"] = round(df["price"] / df["size"])

    # districts_org = df["district"].unique()
    # districts_dict = {k:str(k).replace("","").replace(" ","_").replace("'","") for k in districts_org if k != None}
    # districts_dict[None] = None
    # df["district_renamed"] = df["district"].apply(lambda x: districts_dict[x])
 
    # propertyType_oneHot = pd.get_dummies(df[["propertyType"]], prefix="", prefix_sep="")
    # status = pd.get_dummies(df[["status"]], prefix="state_", prefix_sep="")
    # districts = pd.get_dummies(df[["district_renamed"]], prefix="distr_", prefix_sep="")

    # df_oneHot = pd.concat([df, propertyType_oneHot, districts, status], axis = 1)

    # df_oneHot.to_csv("output/processed_data1.csv")
    # df_oneHot.to_parquet("output/processed_data1.parquet")

    # return df_oneHot





def main():
    df = pd.read_csv("output/okupas.csv")
    process_data(df)

if __name__ == "__main__":
    # save_merged_flats_prices()
    main()
    # features_from_description()
    # get_legal_status_info()
