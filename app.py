import streamlit as st
import pandas as pd
import numpy as np
import urllib.request
import json

max_prices_url = "https://raw.githubusercontent.com/mbalcerzak/idealista-mongo/data-vis/data/most_price_changes.json"

with urllib.request.urlopen(max_prices_url) as url:
    data = json.load(url)


propertyCodes = data.keys()

chosen_code = st.selectbox(
     'How would you like to be contacted?',
     propertyCodes)

if chosen_code:
    chart_data = data[chosen_code]
    print(chart_data)
    st.line_chart(chart_data)