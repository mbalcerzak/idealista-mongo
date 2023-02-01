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


# col1, col2, col3 = st.columns(3)
# col1.metric("Temperature", "70 °F", "1.2 °F")
# col2.metric("Wind", "9 mph", "-8%")
# col3.metric("Humidity", "86%", "4%")
