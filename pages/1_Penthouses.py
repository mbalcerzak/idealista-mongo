import streamlit as st
import urllib.request
import json
import plotly.graph_objects as go
import pandas as pd


df = pd.read_parquet("output/penthouses.parquet")
df_prc = pd.read_parquet("output/penthouses_prc.parquet")


st.title("Penthouses in VLC") 
st.dataframe(df, use_container_width=True)

st.dataframe(df_prc, use_container_width=True)