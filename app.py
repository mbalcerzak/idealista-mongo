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

propertyCodes = list(data.keys())

st.title("Idealista scraper")

chosen_code = st.selectbox(
     'Pick the Property Code',
     propertyCodes,
     index=len(propertyCodes)-1)

st.image(flats_data[chosen_code]['thumbnail'])
st.markdown(f"[Go to the Idealista ad]({flats_data[chosen_code]['url']})")

st.header("House information")


item_list = ('floor', 'price', 'propertyType', 'size', 'exterior', 'rooms', 'bathrooms', 'district', 'neighborhood', 'hasLift', 'parkingSpace', 'priceByArea')
items_present = set(flats_data[chosen_code].keys()).intersection(item_list)

for elem in items_present:
    st.write(f"{elem}: {flats_data[chosen_code][elem]}")

st.subheader("Description")
st.write(flats_data[chosen_code]['description'])

st.header("Price history")
if chosen_code:
    chart_data = data[chosen_code]
    chart_df = pd.DataFrame.from_dict(chart_data, orient='index')
    st.line_chart(chart_df)

distr = flats_data[chosen_code]['district']

col1, col2, col3 = st.columns(3)
col1.metric("Price change: (max->min)", "70 °F", "1.2 °F")
col2.metric(f"Compared to others in {distr}", "9 mph", "-8%")
col3.metric(f"Price per m2", "86%", "4%")
