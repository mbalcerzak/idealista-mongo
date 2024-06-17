import pandas as pd
import typing
from collections import Counter
import re
import spacy
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import joblib 

from src.db_mongo import get_db


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


def save_merged_flats_prices(latest=False, filename="raw_merged") -> None:
    """
    Get prices and flats from two tables and merge by PropertyCode.
    There are be multiple prices per flat
    Choosing option "latest" makes the price of the flat the most recent one
    """
    df_flats, df_prices = get_flats_prq()

    if not latest:
        merged_df = pd.merge(df_flats, df_prices, on="propertyCode")
    else:
        df_prices['date'] = pd.to_datetime(df_prices['date'])

        # Sort the dataframe by 'propertyCode' and 'date' in descending order
        df_sorted = df_prices.sort_values(by=['propertyCode', 'date'], ascending=[True, False])

        # Drop duplicates, keeping the latest date per 'propertyCode'
        df_latest = df_sorted.drop_duplicates(subset='propertyCode', keep='first')

        # Reset index for cleaner output
        df_latest.reset_index(drop=True, inplace=True)
        merged_df = pd.merge(df_flats, df_prices, on="propertyCode")

    merged_df.to_parquet(f"data/{filename}.parquet")


def get_num_price_changes() -> pd.DataFrame:
    """Output a number of times a price was changed per propertyCode"""
    _, df_prices = get_flats_prq()
    c = Counter(df_prices["propertyCode"])
    counter = pd.DataFrame.from_dict(c, orient='index').reset_index()
    flats_price_changed = counter.rename(columns={"index":"propertyCode", 0: "priceChangeCount"})

    return flats_price_changed


def get_terrace_from_description(description: str) -> str:
    if description:
        pattern = re.compile(r'[,.!|\n]')
        sentences = re.split(pattern, description)

        for sentence in sentences:
            terrace_sentence = find_terrace_sentence(sentence)
            if terrace_sentence:
                return terrace_sentence 
            

def find_terrace_sentence(sentence: str) -> str:
    terrace_sentence = None

    # Define a regex pattern to capture the size of the terrace
    # This pattern looks for the word 'terrace' then captures the number
    pattern = r"\bterraza.*?(\d{1,5})\s*(\bmetros|sqm|sq|m|m2|sq)"
    pattern2 = r"\b(\d{1,5})\s*(\bmetros|sqm|sq|m|m2|sq).*?de.*?terraza"

    # Find part that says e.g. 'terraza de aprox tamano 50 metros"
    terrace_match = re.search(pattern, sentence, re.IGNORECASE)
    terrace_match2 = re.search(pattern2, sentence, re.IGNORECASE)

    if terrace_match2:
        terrace_sentence = terrace_match2.group()
    elif terrace_match:
        terrace_sentence = terrace_match.group()

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
    if description and "balcón" in description:
        return True  
    else:
        return False


def get_terrace_yn(description: str) -> bool:
    if description and "terraza" in description:
        return True  
    else:
        return False


def get_legal_status_info(description: str) -> list:
    okupas, alquilado, nudo = False, False, False

    if description:
        words_okupas = ["ocupado", "okupado", "desocupación", 
                        "ocupacion", "ocupas", "ocupación", "no se puede visitar",
                        "no se pueden visitar", "no se puede entrar"]
        words_alquilado = ["alquilado", "alquilada", "alquiladas"]
        words_nudo = ["nuda", "nudo"]

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
    

def get_parking_info(parkingSpace):
    if not parkingSpace:
        return pd.Series([False, False, None])
    
    hasParkingSpace = parkingSpace["hasParkingSpace"]
    parkingIncluded = parkingSpace["isParkingSpaceIncludedInPrice"]
    if not parkingIncluded:
        parkingPrice = int(parkingSpace["parkingSpacePrice"])
    else:
        parkingPrice = None

    return pd.Series([hasParkingSpace, parkingIncluded, parkingPrice])


def clean_description(description: str) -> str:
    if not description: return ""

    description = str(description)
    # Lowercasing: Convert all text to lowercase to maintain uniformity.  
    description = description.lower()
    
    # Remove Punctuation and Special Characters
    description = re.sub(r'[^\w\s]', ' ', description)
    
    # Remove numbers
    description = ''.join([i for i in str(description) if not i.isdigit()])

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


def format_floor(x: str) -> int:
    try:
        return int(x)
    except:
        return 0


def process_data(input:str, head=False) -> pd.DataFrame:
    start_time = time.time()
    df = pd.read_parquet(f"data/{input}.parquet")
    time_read_data = time.time()

    print(f"time_read_data {(time_read_data - start_time)/60:.2f} min.")

    if head:
        df = df.head(100)

    # Clean description from special charaters, stopwords etc.... 
    df["description_clean"] = df["description"].apply(lambda x: clean_description(x))
    time_description = time.time()
    print(f"time_description {(time_description - time_read_data)/60:.2f} min.")

    # Terrace
    df["terrace_sentence"] = df["description"].apply(lambda x: get_terrace_from_description(x))
    df["terraceSize"] = df["terrace_sentence"].apply(lambda x: find_terrace_size(x))
    df["balcon"] = df["description"].apply(lambda x: get_balcon(x))
    time_terrace = time.time()
    print(f"time_terrace {(time_terrace - time_description)/60:.2f} min.")

    # Floor ('bj' means ground floor, 'planta baja')
    df["floor"] = df["floor"].apply(lambda x: format_floor(x))

    # Legal state of the apartment
    df[["okupas", "alquilado", "nudo"]] = df["description"].apply(lambda x: get_legal_status_info(x))
    time_ocupas = time.time()
    print(f"time_ocupas {(time_ocupas - time_terrace)/60:.2f} min.")
  
    # Is the parking space included or available for purchase
    df[["hasParkingSpace", "parkingIncluded", "parkingPrice"]] = df["parkingSpace"].apply(lambda x: get_parking_info(x))
    time_parking = time.time()
    print(f"time_parking {(time_parking - time_ocupas)/60:.2f} min.")
     
    # Number of price changes per apartment 
    # TODO move it to getting data part so it does not load it again
    flats_price_changed = get_num_price_changes()
    df = pd.merge(df, flats_price_changed, on="propertyCode")
    time_price_changes = time.time()
    print(f"time_price_changes {(time_price_changes - time_parking)/60:.2f} min.")
  
    # Price per m2
    df["priceArea"] = round(df["price"] / df["size"])

    # Make district names into features
    districts_org = df["district"].unique()
    districts_dict = {k:str(k).replace(" ","_").replace("'","") for k in districts_org if k != None}
    districts_dict[None] = None
    df["district_renamed"] = df["district"].apply(lambda x: districts_dict[x])
    districts = pd.get_dummies(df[["district_renamed"]], prefix="distr_", prefix_sep="",dtype=int)
    time_districts = time.time()
    print(f"time_districts {(time_districts - time_price_changes)/60:.2f} min.")
  
    propertyType_oneHot = pd.get_dummies(df[["propertyType"]], prefix="", prefix_sep="", dtype=int)
    status = pd.get_dummies(df[["status"]], prefix="state_", prefix_sep="", dtype=int)
 
    df_oneHot = pd.concat([df, propertyType_oneHot, districts, status], axis = 1)
    time_oneHotConcat = time.time()
    print(f"time_oneHotConcat {(time_oneHotConcat - time_districts)/60:.2f} min.")
 
    # Remove columns
    df_oneHot.drop(columns=[
                        "district_renamed", "terrace_sentence", "labels", "has360", "has3DTour",
                        "parkingSpace", "hasVideo", "status", "description", "municipality", 
                        "province", "district", "propertyType", "priceInfo"
                        ], inplace=True, errors="ignore")

    return df_oneHot


def bool_to_num(x):
    if x == True:
        return 1
    else:
        return 0
    

def make_sure_int(x):
    if not x:
        return 0
    try:
        return int(x)
    except:
        return 0


def change_bool_to_num(df) -> pd.DataFrame:
    # Preprocessing and feature engineering
    numerical_features = ['size', 'floor', 'rooms', 'bathrooms', 'numPhotos', 'terraceSize', 'priceChangeCount', 'priceArea', 'parkingPrice']
    boolean_features = ['exterior', 'newDevelopment', 'hasLift', 'balcon', 'okupas', 'alquilado', 'nudo', 'hasParkingSpace', 'parkingIncluded', 
                        'chalet', 'countryHouse', 'flat', 'hasPlan', ]

    all_list = boolean_features + numerical_features

    # Boolean preprocessing (convert to int)
    for col in boolean_features:
        df[col] = df[col].apply(lambda x: bool_to_num(x))

    for col in all_list:
        df[col] = df[col].apply(lambda x: make_sure_int(x))

    return df


def scale_num_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes numerical values and saves Scaler a pkl for later use"""
    scaler = StandardScaler()
    numerical_features = ['size', 'floor', 'rooms', 'bathrooms', 'numPhotos', 'terraceSize', 
                          'priceChangeCount', 'priceArea']
    
    scaler.fit(df[numerical_features])

    df_scaled = scaler.transform(df[numerical_features])

    # Save the fitted scaler to a file for later use
    joblib.dump(scaler, 'models/scaler.pkl')

    df_scaled = pd.DataFrame(df_scaled, columns = numerical_features)

    #### TODO save for the future
    # # Load the saved scaler
    # loaded_scaler = joblib.load('scaler.pkl')

    # # Transform the new incoming data
    # new_data_scaled = loaded_scaler.transform(new_data)

    # # Predict on the new incoming data
    # new_predictions = model.predict(new_data_scaled)

    df[numerical_features] = df_scaled[numerical_features].values

    return df


def td_of_vectorization():
    df = pd.read_parquet(f"output/processed.parquet")
    # Text preprocessing
    text_transformer = Pipeline(steps=[
        ('tfidf', TfidfVectorizer())
    ])


def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    start_time = time.time()
    df = change_bool_to_num(df)
    time_bool = time.time()
    print(f"time_bool {(time_bool - start_time)/60:.2f} min.")

    scaled = scale_num_values(df)
    time_scaled = time.time()
    print(f"time_scaled {(time_scaled - time_bool)/60:.2f} min.")

    return scaled


def main(input_path:str, output_path:str):
    processed = process_data(input_path, head=True)

    processed.to_csv(f"output/{output_path}.csv")
    processed.to_parquet(f"output/{output_path}.parquet")

    processed_norm = normalize_data(processed)
    processed_norm.to_csv("output/processed_norm_all.csv")


if __name__ == "__main__":
    # save_merged_flats_prices(latest=True, filename="raw_merged")
    main(input_path="raw_merged", output_path="processed1")