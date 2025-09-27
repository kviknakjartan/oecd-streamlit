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

col1, col2 = st.columns([3,1])

with col1:
    selected_indicator = st.selectbox("Choose an indicator:", fetcher.getIndicators())

fetcher.updateData(selected_indicator)

min_value, max_value = fetcher.getMinMaxYear(selected_indicator)

if 'years' not in st.session_state:
    st.session_state['years'] = (min_value, max_value)

with col2:
    from_year, to_year = st.slider(
        'Which years are you interested in?',
        min_value=min_value,
        max_value=max_value,
        value=[max(min_value, st.session_state['years'][0]), min(max_value, st.session_state['years'][1])]
    )
st.session_state['years'] = (from_year, to_year)

col1, col2 = st.columns([6,1])

with col1:
    selected_countries = st.multiselect(
        'Select Countries',
        fetcher.getCountries(selected_indicator),
        placeholder = "Choose at least one",
        default = st.session_state['countries']
    )
st.session_state['countries'] = selected_countries

with col2:
    euro_on = st.checkbox("Show when Euro adapted (as of 2025)")

country_data = fetcher.getCountryData(selected_indicator, selected_countries)

# Create figure
p = make_subplots()
for country in selected_countries:
    # Add traces
    p.add_trace(
        go.Scatter(x=country_data.loc[country_data['Reference area'] == country, 'TIME_PERIOD'], \
            y=country_data.loc[country_data['Reference area'] == country, 'OBS_VALUE'], name=country,
            hovertemplate =
            'Value: %{y:.1f}'+
            '<br>Year: %{x:.0f}')
    )
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
st.plotly_chart(p, use_container_width=True)


