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
    

with open("output/most_price_changes.json", "r") as f:
    price_change_data_all = json.load(f)

with open("output/penthouses_price_history.json", "r") as f:
    price_change_data_pent = json.load(f)

with open("output/flat_data.json", "r") as f:
    flats_data_all = json.load(f)   

with open("output/avg_district_prices.json", "r") as f:
    distr_prices_json = json.load(f)

with open("output/avg_neighborhood_prices.json", "r") as f:
    neighborhood_prices_json = json.load(f)

with open("output/avg_district_prices_pent.json", "r") as f:
    distr_prices_pent_json = json.load(f)

with open("output/avg_neighborhood_prices_pent.json", "r") as f:
    neighborhood_prices_pent_json = json.load(f)

with open("output/max_price_diffs.json", "r") as f:
    max_price_diffs = json.load(f)

with open("output/cheap_penthouses.json", "r") as f:
    cheap_penthouses = json.load(f)

propertyCodes = list(flats_data_all.keys())
propertyCodesPent = list(cheap_penthouses.keys())

st.title("Idealista scraper") 

max_price_diffs = {x[0]:x[1] for x in max_price_diffs}

penthouse = st.radio("Type of property",('Penthouse',  'All'), index=1)

if penthouse == "Penthouse":
    # propCodes = [f"{k} ({v} %)" for k,v in max_price_diffs.items() if k in propertyCodesPent]
    propCodes = [f"{k} ({round(v['distrPriceDiff']*100):+d} %)" for k,v in cheap_penthouses.items() if k in propertyCodesPent]
    flats_data = cheap_penthouses
    price_change_data = price_change_data_pent
else:
    propCodes = [f"{k} ({v} %)" for k,v in max_price_diffs.items() if k in propertyCodes]
    flats_data = flats_data_all
    price_change_data = price_change_data_all

chosen_code = st.selectbox(
     'Pick the Property Code',
     propCodes
     )

chosen_code = chosen_code.split(" ")[0]

flat = flats_data[chosen_code]
flat_price_change = price_change_data[chosen_code]
size = flat["size"]

title = flat['suggestedTexts']["title"]
subtitle = flat['suggestedTexts']["subtitle"]


if 'thumbnail' in flat:
    img_url = flat['thumbnail']
    st.markdown(f"![img]({img_url})")
st.markdown(f"[Go to the Idealista ad]({flat['url']})")

st.header(f"{title}: {subtitle}")

characteristics = get_characteristics()  
st.markdown(characteristics)

if 'description' in flat:
    with st.expander("See description"):
        # st.subheader("Description")
        st.write(flat['description'])

st.header("Price history")

################################  area  #########################################

def get_neighbourhood_prices(df):
    neighborhood = flat['neighborhood']
    neighborhood_prices = df[neighborhood]
    latest_n = max(list(neighborhood_prices.keys()))
    neighborhood_price = neighborhood_prices[latest_n]["price"]

    neigh_prices = [x["price"] for x in list(neighborhood_prices.values())]
    neigh_prices_size = [int(x*size) for x in neigh_prices]
    neigh_dates = list(neighborhood_prices.keys())

    neigh_count = [f'Num flats: {x["count"]}' for x in list(neighborhood_prices.values())]

    neigh_diff = (max(neigh_prices) - min(neigh_prices))/max(neigh_prices)
    neigh_diff = round(neigh_diff*100, 2)

    return neigh_dates, neigh_prices_size, neighborhood_price, neigh_diff, neighborhood, neigh_count


def get_distr_prices(df):
    district = flat['district']
    district_prices = df[district]
    latest_d = max(list(district_prices.keys()))
    district_price = district_prices[latest_d]["price"]

    distr_prices = [x["price"] for x in list(district_prices.values())]
    distr_prices_size = [int(x*size) for x in distr_prices]
    distr_dates = list(district_prices.keys())

    distr_count = [f'Num flats: {x["count"]}' for x in list(district_prices.values())]

    distr_diff = (max(distr_prices) - min(distr_prices))/max(distr_prices)
    distr_diff = round(distr_diff*100, 2)

    return distr_dates, distr_prices_size, district_price, distr_diff, district, distr_count

if 'neighborhood' in list(flat):
    neigh_dates, neigh_prices_size, neighborhood_price, neigh_diff, neighborhood, neigh_count = get_neighbourhood_prices(neighborhood_prices_json)
    neigh_dates_p, neigh_prices_size_p, neighborhood_price_p, neigh_diff_p, neighborhood_p, neigh_count_p = get_neighbourhood_prices(neighborhood_prices_pent_json)
else:
    neighborhood = None

if 'district' in list(flat):
    distr_dates, distr_prices_size, district_price, distr_diff, district, distr_count = get_distr_prices(distr_prices_json)
    distr_dates_p, distr_prices_size_p, district_price_p, distr_diff_p, district_p, distr_count_p = get_distr_prices(distr_prices_pent_json)
else:
    district = None

flat_price_m2 = flat['priceByArea']

flat_prices = list(flat_price_change.values())
flat_dates = list(flat_price_change.keys())

current_price = flat_price_change[max(flat_dates)]


st.subheader(f" current price: {fmt_price(current_price)}")
col1, col2 = st.columns(2)
with col1:
    distr_chng_show = st.radio("Show avg district price changes",('Yes', 'No'),index=1, )
with col2:
    distr_chng_show_p = st.radio("Show avg penthouse prices",('Yes', 'No'),index=1, )

fig = go.Figure()
fig.add_trace(go.Scatter(x=flat_dates, y=flat_prices, name="Flat"))
if distr_chng_show == "Yes":
    fig.add_trace(go.Scatter(x=neigh_dates, y=neigh_prices_size, name="neighborhood", text=neigh_count))
    fig.add_trace(go.Scatter(x=distr_dates, y=distr_prices_size, name="district", text=distr_count))
    fig.update_layout(hovermode='x unified')
if distr_chng_show_p == "Yes":
    fig.add_trace(go.Scatter(x=neigh_dates_p, y=neigh_prices_size_p, name="neighborhood (Penthouses)", text=neigh_count_p))
    fig.add_trace(go.Scatter(x=distr_dates_p, y=distr_prices_size_p, name="district (Penthouses)", text=distr_count_p))
    fig.update_layout(hovermode='x unified')  


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


st.subheader("Penthouses stats")
col1, col2, col3 = st.columns(3)
col1.metric(
    "Price of this flat per m2", 
    fmt_price(flat_price_m2))
if district:
    col2.metric(
        f"District per m2 ({district})",  
        f"{fmt_price(district_price_p)}")
if neighborhood:
    col3.metric(
        f"Neighborhood per m2 ({neighborhood})", 
        f"{fmt_price(neighborhood_price_p)}")
