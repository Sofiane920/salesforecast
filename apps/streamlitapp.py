from preprocess import(read_prod)
import altair as alt
import streamlit as st
from mlforecast import MLForecast
from mlforecast.target_transforms import Differences
from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
from mlforecast.callbacks import SaveFeatures



st.set_page_config(page_title="Analyse des Données du Restaurant", layout="wide")
st.title("Analyse des Données Paiement et production")
st.sidebar.header("Filtres")


df = read_prod()

st.write(df.head())


daily_df = df.groupby(['Jour', 'Produit']).agg(
    y = ('Quantité', 'sum')
).reset_index().sort_values(by=['Jour']).reset_index(drop=True)

daily_df = daily_df.rename(columns={"Jour": 'ds', 'Produit': 'unique_id'})
daily_df['ds'] = pd.to_datetime(daily_df['ds'])

st.write(daily_df.head(5))
st.write(daily_df.isnull().sum())
st.write(daily_df.isna().sum())
save_features = SaveFeatures()

fcst = MLForecast(
    models=LinearRegression(),
    freq='D',  
    lags=[7]
)
fcst.fit(daily_df)

preds = fcst.predict(5, before_predict_callback=save_features)

#features = save_features.get_features()
#st.write(features.isna().sum())