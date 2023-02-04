import streamlit as st
import pandas as pd
import numpy as np
import urllib.request
import json
import plotly.graph_objects as go


def fmt_price(x:int):
    """Format price into 'XX XXX €' """
    return f"{round(x):,} €".replace(","," ")


max_prices_url = "https://raw.githubusercontent.com/mbalcerzak/idealista-mongo/data-vis/output/most_price_changes.json"

with urllib.request.urlopen(max_prices_url) as url:
    data = json.load(url)

with open("output/flat_data.json", "r") as f:
    flats_data = json.load(f)   

with open("output/avg_district_prices.json", "r") as f:
    area_prices = json.load(f)

propertyCodes = list(data.keys())

st.title("Idealista scraper")

chosen_code = st.selectbox(
     'Pick the Property Code',
     propertyCodes,
     index=len(propertyCodes)-1)

flat = flats_data[chosen_code]

st.image(flat['thumbnail'])
st.markdown(f"[Go to the Idealista ad]({flat['url']})")

st.header("House information")

item_list = ('floor', 'propertyType', 'size', 'exterior', 'rooms', 'bathrooms', 'areaict', 'area', 'hasLift', 'parkingSpace')
items_present = set(flat.keys()).intersection(item_list)

characteristics = ""
for elem in items_present:
    characteristics += f"- {elem}: {flat[elem]}\n"
    
st.markdown(characteristics)

st.subheader("Description")
st.write(flat['description'])

st.header("Price history")
st.subheader(f" current price: {fmt_price(flat['price'])}")

if chosen_code:
    chart_data = data[chosen_code]
    dates = list(chart_data.keys())
    prices = list(chart_data.values())
    fig = go.Figure([go.Scatter(x=dates, y=prices)])
    st.plotly_chart(fig)

#########################################   prices  #########################################

prices = list(chart_data.values())
max_p = max(prices)
min_p = min(prices)


diff = min_p - max_p
diff_prc = round(diff/max_p*100,2)


################################  area  #########################################

area_list = sorted(list(area_prices.keys()))
area = flat['neighborhood']
area_price = area_prices[area]
flat_price_m2 = flat['priceByArea']

################################   price m2   #########################################


col1, col2, col3 = st.columns(3)
col1.metric("Price change: (max->min)", fmt_price(diff), f"{diff_prc} %", delta_color="inverse")
col2.metric(f"Price per m2 of this flat",  f"{fmt_price(flat_price_m2)}")
col3.metric(f"Average price in {area}", f"{fmt_price(area_price)}")
