import streamlit as st
import os
import snowflake.connector
from dotenv import load_dotenv
import altair as alt
import pandas as pd

load_dotenv()

# Load Snowflake connection parameters
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

def convert_hectares_to_sqm(value):
    if 'ha' in value:
        value_in_hectares = float(value.replace('ha', '').strip())
        result = value_in_hectares * 10000
        return int(result) if result.is_integer() else result
    else:
        return float(value)

def clean_size_column(series):
    cleaned = (
        series.astype(str)
        .str.lower()
        .str.replace("m²", "", regex=False)
        .str.replace("r", "", regex=False)
        .str.replace(" ", "")
        .str.replace(",", "")
        .replace(["poa", "na", "n/a", "", None], 0)
    )
    return cleaned.apply(lambda x: convert_hectares_to_sqm(x) if isinstance(x, str) else x)

# Connect and query
conn = connect_to_snowflake()

if conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM PROP24.PROP24_LISTING_DATA.PROPERTIES;")
    df = cursor.fetch_pandas_all()

    if not df.empty:
        st.write("🏠 Full Property Data")
        st.dataframe(df)

        # Add property lookup section
        st.subheader("🔎 Find Property by Listing Number")
        listing_number_input = st.text_input("Enter the listing number:")
        if listing_number_input:
            listing_number_input = listing_number_input.strip()
            matching_row = df[df['LISTING_NUMBER'].astype(str) == listing_number_input]
            if not matching_row.empty:
                if 'URL' in matching_row.columns:
                    property_url = matching_row['URL'].values[0]
                else:
                    property_url = f"https://www.property24.com/Listing/{listing_number_input}"
                st.markdown(f"🔗 [Click to view property listing]({property_url})", unsafe_allow_html=True)
            else:
                st.warning("No property found for that listing number.")

        # Clean price and sizes
        df['PRICE_CLEAN'] = clean_size_column(df['PRICE']).dropna().astype(float)
        df['ERF_SIZE_CLEAN'] = clean_size_column(df['ERF_SIZE']).astype(float)
        df['FLOOR_SIZE_CLEAN'] = clean_size_column(df['FLOOR_SIZE']).astype(float)
        df['SUB_DIVIDABLE_AREA'] = df['ERF_SIZE_CLEAN'] - df['FLOOR_SIZE_CLEAN']

        # # Chart 1: Sub-dividable
        # df_subdividable = df[df['SUB_DIVIDABLE_AREA'] >= 500].copy()
        # df_subdividable = df_subdividable.sort_values(by='SUB_DIVIDABLE_AREA', ascending=False)

        # st.subheader("📊 Sub-dividable Properties (≥ 500 m² extra space)")
        # chart1 = alt.Chart(df_subdividable).mark_bar().encode(
        #     x=alt.X('LISTING_NUMBER:N', sort='-y', title='Property Address'),
        #     y=alt.Y('SUB_DIVIDABLE_AREA:Q', title='Sub-dividable Area (m²)'),
        #     tooltip=['LISTING_NUMBER', 'SUB_DIVIDABLE_AREA']
        # ).properties(
        #     width=600,
        #     height=400
        # )
        # st.altair_chart(chart1, use_container_width=True)


    # --- List Top 10 Properties by ERF-to-Floor Ratio (cleaned and simplified)
    # --- Filter: Only use properties where LIFESTYLE is 'na' or blank
      # Filter properties with LIFESTYLE == 'na' and TYPE_OF_PROPERTY != 'Apartment / Flat'
        df_filtered = df[
            (df['LIFESTYLE'].str.lower() == 'na') &
            (df['TYPE_OF_PROPERTY'].str.lower() != 'apartment / flat')
        ].copy()

        # Calculate ERF to FLOOR size ratio
        df_filtered['ERF_TO_FLOOR_RATIO'] = df_filtered['ERF_SIZE_CLEAN'] / df_filtered['FLOOR_SIZE_CLEAN']
        df_filtered.replace([float('inf'), -float('inf')], pd.NA, inplace=True)
        df_filtered.dropna(subset=['ERF_TO_FLOOR_RATIO'], inplace=True)

        # Sort and drop duplicates
        df_top_ratio = df_filtered.sort_values(by='ERF_TO_FLOOR_RATIO', ascending=False)
        df_top_ratio = df_top_ratio.drop_duplicates(subset='LISTING_NUMBER')

        # Format for display
        df_display = df_top_ratio[['LISTING_NUMBER', 'ERF_SIZE_CLEAN', 'FLOOR_SIZE_CLEAN', 'ERF_TO_FLOOR_RATIO']].copy()
        df_display.columns = ['LISTING_NUMBER', 'ERF_SIZE', 'FLOOR_SIZE', 'ERF_TO_FLOOR_RATIO']
        df_display['ERF_SIZE'] = df_display['ERF_SIZE'].astype(int)
        df_display['FLOOR_SIZE'] = df_display['FLOOR_SIZE'].astype(int)
        df_display['ERF_TO_FLOOR_RATIO'] = df_display['ERF_TO_FLOOR_RATIO'].round(4)

        # Show top 10
        st.subheader("🏡 Top 10 Properties by ERF to Floor Size Ratio")
        st.markdown("These properties have the largest open land around the house. "
                    "**Copy the listing number below** and paste it in the box to view more details.")
        st.dataframe(df_display.head(10))

        # --- Search box
        listing_input = st.text_input("🔍 Enter a Listing Number to view more details:")

        if listing_input:
            listing_info = df[df['LISTING_NUMBER'].astype(str) == listing_input.strip()]
            
            if not listing_info.empty:
                st.subheader(f"📄 Details for Listing Number: {listing_input}")
                st.write(listing_info.T)
            else:
                st.warning("⚠️ Listing number not found. Please check and try again.")

        # Chart 2: Top 5 ERF size
        df_top5_erf = df.sort_values(by='ERF_SIZE_CLEAN', ascending=False).head(5)
        df_melted = df_top5_erf.melt(
            id_vars='ADDRESS',
            value_vars=['ERF_SIZE_CLEAN', 'FLOOR_SIZE_CLEAN'],
            var_name='Size Type',
            value_name='Square Meters'
        )
        df_melted['Size Type'] = df_melted['Size Type'].map({
            'ERF_SIZE_CLEAN': 'ERF Size',
            'FLOOR_SIZE_CLEAN': 'Floor Size'
        })

        st.subheader("🏡 ERF vs Floor Size for Top 5 Largest Properties")
        chart2 = alt.Chart(df_melted).mark_bar().encode(
            x=alt.X('ADDRESS:N', title='Property Address'),
            y=alt.Y('Square Meters:Q', title='Size (m²)'),
            color='Size Type:N',
            column='Size Type:N',
            tooltip=['ADDRESS', 'Size Type', 'Square Meters']
        ).properties(
            width=150,
            height=400
        )
        st.altair_chart(chart2, use_container_width=True)

else:
    st.error("🚫 Failed to connect to Snowflake.")
