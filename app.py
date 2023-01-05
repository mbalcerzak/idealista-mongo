import streamlit as st
import pandas as pd
import numpy as np
import urllib.request
import json

max_prices_url = "https://raw.githubusercontent.com/mbalcerzak/idealista-mongo/data-vis/output/most_price_changes.json"

with urllib.request.urlopen(max_prices_url) as url:
    data = json.load(url)

with open("output/flat_data.json", "r") as f:
    flats_data = json.load(f)   

propertyCodes = data.keys()

chosen_code = st.selectbox(
     'Pick the Property Code',
     propertyCodes)

st.header("House information")
flats_data[chosen_code]


st.header("Price history")
if chosen_code:
    chart_data = data[chosen_code]
    chart_df = pd.DataFrame.from_dict(chart_data, orient='index')
    st.line_chart(chart_df)


