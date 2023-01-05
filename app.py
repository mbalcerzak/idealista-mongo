import streamlit as st
import pandas as pd
import numpy as np
import urllib.request
import json

max_prices_url = "https://raw.githubusercontent.com/mbalcerzak/idealista-mongo/data-vis/data/most_price_changes.json"

with urllib.request.urlopen(max_prices_url) as url:
    data = json.load(url)


propertyCodes = [x["propertyCode"] for x in data]

option = st.selectbox(
     'How would you like to be contacted?',
     propertyCodes)

if option:

    chart_data = None

    st.line_chart(chart_data)