import streamlit as st
import pymongo


"""
with open("creds_mongo_mab.json", "r") as f:
    creds_mongo = json.load(f)

username = creds_mongo["username"]
password = creds_mongo["password"]

cluster = MongoClient(f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority")
db = cluster["flats"]
collection_flats = db["_flats"]
collection_prices = db["_prices"]
"""


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# Pull data from the collection.
# Uses st.experimental_memo to only rerun when the query changes or after 20 min.
@st.experimental_memo(ttl=1200)
def get_data():
    db = client.mydb
    items = db.mycollection.find()
    items = list(items)  # make hashable for st.experimental_memo
    return items

items = get_data()

# Print results.
for item in items:
    st.write(f"{item['name']} has a :{item['pet']}:")