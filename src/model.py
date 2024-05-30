import pandas as pd
from db_mongo import get_db
from collections import Counter
from sklearn.preprocessing import OneHotEncoder
import typing
from collections import Counter
import re
import nltk
from nltk.stem import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer
import spacy
# nlp = spacy.load("es_core_news_sm")


def get_flats_prq() -> typing.List[pd.DataFrame]:
    """Returns flat info and price collection from Mongo database in a form of DataFrame"""
    db = get_db()
    collection_flats = db["_flats"]
    collection_prices = db["_prices"]
 
    flats = collection_flats.find()
    prices = collection_prices.find()

    cols_flats = [
        'propertyCode', 'propertyType',  'size', 'exterior', 'rooms', 'district',
        'bathrooms', 'province', 'municipality', 'description', 'status', 'newDevelopment'
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


def find_terrace_size(sentence: str) -> typing.List[str]:
    # Define a regex pattern to capture the size of the terrace
    # This pattern looks for the word 'terrace' then captures the number
    pattern = r"\bterraza.*?(\d{1,5})\s*(\bmetros|sqm|sq|m|m2|sq),?"

    terrace_sentence, terrace_size = None, None

    # Find part that says e.g. 'terraza de aprox tamano 50 metros"
    terrace_match = re.search(pattern, sentence, re.IGNORECASE)

    if terrace_match:
        # Find the number which represents the terrace size
        pattern_num = re.compile(r'\d{1,5}')
        terrace_sentence = terrace_match.group()
        terrace_size_match = pattern_num.search(terrace_sentence)

        terrace_size = terrace_size_match.group()

    if terrace_sentence:
        print("-------------")
        print(sentence)
        print(f"{terrace_sentence=}, {terrace_size=}")

    return terrace_sentence, terrace_size
    

def get_features_from_description(desc: list):
    # long_string = ' '.join(desc)

    n = -1

    print(f"{len(desc)=}")

    for description in desc:
        n += 1
        # description = description.replace(".", ",")
        if description:
            sentences = description.split(". ")

            for sentence in sentences:
                # print(sentence)

                terrace_sentence, terrace_size = find_terrace_size(sentence)




def process_data(data: pd.DataFrame) -> pd.DataFrame:
    
    # df = data.head()

    get_features_from_description(data["description"])

    # c = Counter(merged_df["propertyCode"])
    # counter = pd.DataFrame.from_dict(c, orient='index').reset_index()
    # flats_price_changed = counter.rename(columns={"index":"propertyCode", 0: "priceChangeCount"})

    # df = pd.merge(merged_df, flats_price_changed, on="propertyCode")
    # df["priceArea"] = round(df["price"] / df["size"])

    # One Hot Encoding
    # districts_org = df["district"].unique()
    # districts_dict = {k:str(k).replace("- ","").replace(" ","_").replace("'","") for k in districts_org if k != None}
    # districts_dict[None] = None

    # df["district_renamed"] = df["district"].apply(lambda x: districts_dict[x])
    # df["exterior"] = df["exterior"].apply(lambda x: 1 if True else 0)

    # propertyType_oneHot = pd.get_dummies(df[["propertyType"]], prefix="", prefix_sep="")
    # # district_oneHot = pd.get_dummies(df[["district_renamed"]], prefix="", prefix_sep="")
    # status = pd.get_dummies(df[["status"]], prefix="", prefix_sep="")

    # df_oneHot = pd.concat([df, propertyType_oneHot, district_oneHot, status], axis = 1)

    # for distr in sorted(df["district_renamed"].unique()):
    #     print(distr)

    # if 'nan' in list(df_oneHot):
    #     df_oneHot.drop('nan', axis=1, inplace=True)

    # return df_oneHot


def main():
    df = pd.read_parquet("data/raw_merged.parquet")
    process_data(df)

if __name__ == "__main__":
    main()

    # sentence = "Atico con encanto en el centro de Valencia, con una terraza of 25m aprox"
    # sen1 = "The apartment has a terraza of 50.09 metros"
    # sen2 = "The apartment has a terrazeee of 50.09 m."
    # sen3 = " Because so much 45 terraza 7.3 m2"
    # sen4 = "The apartment has a terraza y 17 habitaciones y 42metros"
    # sen5 = "The apartment has a terraza of 50.09    m"

    # for sentence in [sen1, sen2, sen3, sen4, sen5]:
    #     find_terrace_size(sentence)