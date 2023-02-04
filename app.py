import streamlit as st
import urllib.request
import json
import plotly.graph_objects as go


def fmt_price(x:int):
    """Format price into 'XX XXX €' """
    return f"{round(x):,} €".replace(","," ")


def get_characteristics():
    item_list = ('floor', 'propertyType', 'size', 'exterior', 'rooms', 'bathrooms', 
                'areaict', 'area', 'hasLift', 'parkingSpace', 'district', 'neighborhood')
    items_present = set(flat.keys()).intersection(item_list)

    characteristics = ""
    for elem in items_present:
        characteristics += f"- {elem}: **{flat[elem]}**\n"
    return characteristics
    
mab_github = "https://raw.githubusercontent.com/mbalcerzak/idealista-mongo/data-vis"
max_prices_url = f"{mab_github}/output/most_price_changes.json"

with urllib.request.urlopen(max_prices_url) as url:
    price_change_data = json.load(url)

with open("output/flat_data.json", "r") as f:
    flats_data = json.load(f)   

with open("output/avg_district_prices.json", "r") as f:
    distr_prices = json.load(f)

with open("output/avg_neighborhood_prices.json", "r") as f:
    neighborhood_prices = json.load(f)

propertyCodes = list(price_change_data.keys())

st.title("Idealista scraper") 

chosen_code = st.selectbox(
     'Pick the Property Code',
     propertyCodes
     )

flat = flats_data[chosen_code]
flat_price_change = price_change_data[chosen_code]
size = flat["size"]

img_url = flat['thumbnail']
st.markdown(f"![img]({img_url})")
st.markdown(f"[Go to the Idealista ad]({flat['url']})")

st.header("House information")

characteristics = get_characteristics()  
st.markdown(characteristics)

st.subheader("Description")
st.write(flat['description'])
st.header("Price history")

################################  area  #########################################

if 'neighborhood' in list(flat):
    neighborhood = flat['neighborhood']
    neighborhood_prices = neighborhood_prices[neighborhood]
    latest_n = max(list(neighborhood_prices.keys()))
    neighborhood_price = neighborhood_prices[latest_n]["price"]

    neigh_prices = [x["price"] for x in list(neighborhood_prices.values())]
    neigh_prices_size = [x*size for x in neigh_prices]
    neigh_dates = list(neighborhood_prices.keys())

    neigh_diff = (max(neigh_prices) - min(neigh_prices))/max(neigh_prices)
    neigh_diff = round(neigh_diff*100, 2)
else:
    neighborhood = None

if 'district' in list(flat):
    district = flat['district']
    district_prices = distr_prices[district]
    latest_d = max(list(district_prices.keys()))
    district_price = district_prices[latest_d]["price"]

    distr_prices = [x["price"] for x in list(district_prices.values())]
    distr_prices_size = [x*size for x in distr_prices]
    distr_dates = list(district_prices.keys())

    distr_diff = (max(distr_prices) - min(distr_prices))/max(distr_prices)
    distr_diff = round(distr_diff*100, 2)
else:
    district = None

flat_price_m2 = flat['priceByArea']

flat_prices = list(flat_price_change.values())
flat_dates = list(flat_price_change.keys())

current_price = flat_price_change[max(flat_dates)]
st.subheader(f" current price: {fmt_price(current_price)}")
# distr_chng_show = st.radio("Show district price changes",('Yes', 'No'),index=1, )

fig = go.Figure()
fig.add_trace(go.Scatter(x=flat_dates, y=flat_prices, name="Flat"))
# if distr_chng_show == "Yes":
#     fig.add_trace(go.Scatter(x=neigh_dates, y=neigh_prices_size, name="neighborhood"))
#     fig.add_trace(go.Scatter(x=distr_dates, y=distr_prices_size, name="district"))

st.plotly_chart(fig)

################################   prices  ######################################
prices = list(flat_price_change.values())

max_p = max(prices)
min_p = min(prices)

diff = min_p - max_p
diff_prc = round(diff/max_p*100,2)

###############################   price m2   ####################################

col1, col2, col3 = st.columns(3)
col1.metric(
    "Price change: (max → min)", 
    fmt_price(diff), 
    f"{diff_prc} %", 
    delta_color="inverse")
if district:
    col2.metric(
        f"District per m2 ({district})",  
        f"{fmt_price(district_price)}", 
        f"{distr_diff} %", 
        delta_color="inverse")
if neighborhood:
    col3.metric(
        f"Neighborhood per m2 ({neighborhood})", 
        f"{fmt_price(neighborhood_price)}", 
        f"{neigh_diff} %",
         delta_color="inverse")
