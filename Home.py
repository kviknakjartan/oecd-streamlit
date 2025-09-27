import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from get_data import DataFetcher

if 'countries' not in st.session_state:
    st.session_state['countries'] = []

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='OECD Dashboard',
    page_icon='📈',
    layout='wide',
    initial_sidebar_state='collapsed'
)

fetcher = DataFetcher()

############################# Top row: indicator selection and year range selection
col1, col2 = st.columns([3,1])

with col1:
    # Select indicator
    selected_indicator = st.selectbox("Choose an indicator:", fetcher.getIndicators())

# Download data from OECD for selected indicator if it hasn't been done before
fetcher.updateData(selected_indicator)
# Set year range
min_value, max_value = fetcher.getMinMaxYear(selected_indicator)

# Set year range
with col2:
    from_year, to_year = st.slider(
        'Which years are you interested in?',
        min_value=min_value,
        max_value=max_value,
        value=[min_value, max_value]
    )

############################# Second row: select countries and checkbox for showing when euro was adopted on plot
def update_countries():
    # Update permanent key-value pair from temporary one.
    st.session_state.countries = st.session_state.temp_countries

# Get value for widget from permanent key.
st.session_state.temp_countries = st.session_state.countries

col1, col2 = st.columns([6,1])
# Multiselect countries
with col1:
    selected_countries = st.multiselect(
        'Select Countries',
        fetcher.getCountries(selected_indicator),
        placeholder = "Choose at least one",
        key="temp_countries", 
        on_change=update_countries
    )
# Euro checkbox
with col2:
    euro_on = st.checkbox("Show when Euro adapted (as of 2025)")

############################### Plot the data
# Get data for selected indicator and selected countries
country_data = fetcher.getCountryData(selected_indicator, selected_countries)

# Create figure
p = make_subplots()
# Unit symbol to show with hover data
unit_symbol = fetcher.getUnitSymbol(selected_indicator)
if unit_symbol is None:
    hover = 'Value: %{y:.1f}'+'<br>Year: %{x:.0f}'
else:
    hover = 'Value: %{y:.1f}'+unit_symbol+'<br>Year: %{x:.0f}'

for country in selected_countries:
    # Add trace for each selected country
    p.add_trace(
        go.Scatter(x=country_data.loc[country_data['Reference area'] == country, 'TIME_PERIOD'], \
            y=country_data.loc[country_data['Reference area'] == country, 'OBS_VALUE'], name=country,
            hovertemplate = hover)
    )
    # Show if and when euro was adopted by country
    if euro_on:
        euro_date = fetcher.getEuroYear(country)
        if euro_date is not None:
            p.add_trace(
                go.Scatter(x=[euro_date], \
                    y=country_data.loc[(country_data['Reference area'] == country) & \
                        (country_data['TIME_PERIOD'] == euro_date), 'OBS_VALUE'], 
                    name=f'{country}_euro',
                    hoverinfo='skip',
                    showlegend=False,
                    mode='markers',
                    marker=dict(
                                size=10,
                                color='gold',
                                symbol='diamond'))
            )
# Set x-axis range and configure legend
p.update_layout(
        xaxis=dict(range=[from_year, to_year]),
        legend=dict(
            x=0.1,  # x-position (0.1 is near left)
            y=0.7,  # y-position (0.9 is near top)
            xref="container",
            yref="container",
            orientation = 'h'
    )
)
# Set x-axis title
p.update_xaxes(title_text="Year")

# Set y-axes title
p.update_yaxes(title_text=fetcher.getUnit(selected_indicator))

# Add an annotation to act as a clickable link
p.add_annotation(
    text=f"<a href='{fetcher.getLink(selected_indicator)}'>{selected_indicator}</a>", # The text displayed for the link
    xref="paper", yref="paper",
    x=0.50, y=1.30, # Position relative to the plot area
    showarrow=False,
    font=dict(color="blue", size=24),
    align="center"
)
st.plotly_chart(p, use_container_width=True)

st.caption('Data from [https://data-explorer.oecd.org/](https://data-explorer.oecd.org/)')




