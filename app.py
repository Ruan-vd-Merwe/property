import streamlit as st
import os
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import altair as alt

load_dotenv()

# ─── Snowflake Connection ──────────────────────────────────────────────
snowflake_conn_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

def connect_to_snowflake():
    try:
        return snowflake.connector.connect(**snowflake_conn_params)
    except Exception as e:
        st.error(f"❌ Snowflake connection error: {e}")
        return None

# ─── Cleaning Utility ──────────────────────────────────────────────────
def clean_numeric(value):
    try:
        return float(str(value).lower().replace("m²", "").replace(",", "").replace(" ", "").replace("r", "").replace("ha", "0000"))
    except:
        return 0.0

# ─── Main App ──────────────────────────────────────────────────────────
st.title("🏡 Property Explorer")

conn = connect_to_snowflake()

if conn:
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM PROP24.PROP24_LISTING_DATA.PROPERTIES;")
        df = cursor.fetch_pandas_all()

        if not df.empty:
            # Clean and convert columns
            df['PRICE_CLEAN'] = df['PRICE'].apply(clean_numeric)
            df['SIZE_CLEAN'] = df['SIZE'].apply(clean_numeric)

            st.subheader("📊 Full Property Dataset")
            st.dataframe(df)

            # Filter section
            st.subheader("🔍 Filter Properties")
            location_filter = st.selectbox("Location", options=['All'] + sorted(df['LOCATION'].dropna().unique().tolist()))
            if location_filter != 'All':
                df = df[df['LOCATION'] == location_filter]

            # Price range
            min_price, max_price = st.slider("Price Range", min_value=0, max_value=int(df['PRICE_CLEAN'].max()), value=(0, int(df['PRICE_CLEAN'].max())))
            df = df[(df['PRICE_CLEAN'] >= min_price) & (df['PRICE_CLEAN'] <= max_price)]

            st.subheader("🏘️ Filtered Properties")
            st.dataframe(df[['PRICE', 'SIZE', 'LOCATION', 'BEDROOMS', 'BATHROOMS', 'PARKING']])

            # Visualization
            st.subheader("📈 Price vs Size")
            chart = alt.Chart(df).mark_circle(size=60).encode(
                x=alt.X('SIZE_CLEAN:Q', title='Size (m²)'),
                y=alt.Y('PRICE_CLEAN:Q', title='Price (ZAR)'),
                color='LOCATION:N',
                tooltip=['PRICE', 'SIZE', 'LOCATION', 'BEDROOMS', 'BATHROOMS', 'PARKING']
            ).interactive()

            st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"🚨 Query failed: {e}")
else:
    st.error("🚫 Could not connect to Snowflake.")
