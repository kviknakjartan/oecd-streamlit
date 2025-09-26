import sys
import requests
import pandas as pd
import numpy as np
from io import StringIO, BytesIO


class DataFetcher():
    def __init__(self):
        self.batchDict = {'naag_ch1' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_I,/A.ZAF+IDN+IND+CHN+BRA+OECD+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B1GQ_R_POP.USD_PPP_PS.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - GDP data.csv"},
                          'naag_ch2' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_II,1.0/A.ZAF+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B5N_R.IX.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - income data.csv"},
                          'naag_ch3' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_III,1.0/A.ZAF+RUS+IDN+IND+CHN+BRA+OECD+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.P32S13+P31S13+P3S1M_POP.PT_P3_POP_PPP_OECD+PT_B1GQ.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - expenditure data.csv"},
                          'naag_ch3a' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_III_CG,1.0/A.ZAF+RUS+IDN+IND+OECD+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B11_CG..?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - exports data.csv"},
                          'naag_ch4' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG_IV@DF_NAAG_IV,1.0/A.ZAF+RUS+IDN+IND+CHN+BRA+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B1G_GR.C+_T.PC.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - production data.csv"},
                          'naag_ch5' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_V,1.0/A.ZAF+RUS+CHN+BRA+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B8NS1M+LES1M_FD4+B6GS1M_POP.PT_B6N_S1M+USD_PPP_PS.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - households data.csv"},
                          'naag_ch6' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG_VI@DF_NAAG_EXP,1.0/A.ZAF+RUS+IDN+CHN+BRA+EU+EA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B9S13+LES13_FD3+ODAS13_D2_D5_D91_D611_D613+OTRS13+OTES13..PT_B1GQ.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - government data.csv"},
                          'naag_ch6a' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG_VI@DF_NAAG_OTEF,1.0/A.EU+EA+AUS+AUT+BEL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+NLD+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+GBR+USA.OTES13F.GF07+GF02+GF01+GF09+_T..?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - sector data.csv"},
                          'naag_ch7' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_VII,1.0/A.RUS+BRA+AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.LES12_FD4_F5LI+LES11_FD4_B2G.FCTR_B2G+FCTR_LI_F51.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - corporations data.csv"},
                          'naag_ch8' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_VIII,1.0/A.EU+EA+AUS+AUT+BEL+CAN+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+GBR+USA.LE_N11N.IX.?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - capital data.csv"},
                          'unempl' : {'url' : "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,1.0/ROU+BGR+HRV+OECD+EU27_2020+EA20+G7+USA+GBR+TUR+CHE+SWE+ESP+SVN+SVK+PRT+POL+NOR+NZL+NLD+MEX+LUX+LTU+LVA+KOR+JPN+ITA+ISR+IRL+ISL+HUN+GRC+DEU+FRA+FIN+EST+DNK+CZE+CRI+COL+CHL+CAN+BEL+AUT+AUS..._Z.Y._T.Y_GE15..A?startPeriod=1925&dimensionAtObservation=AllDimensions&format=csvfilewithlabels",
                                        'test_file': "OECD - employment data.csv"}}

        self.dataDict = { 'GDP: Real gross domestic product per capita' : {'batch' : "naag_ch1",
                                                            'measure' : "B1GQ_R_POP",
                                                            'unit' : 'US dollars per person, PPP converted',
                                                            'link' : "https://www.oecd.org/en/data/indicators/real-gross-domestic-product-gdp.html",
                                                            'link_name' : 'Real Gross Domestic Product (GDP)'}, 
                     'Income: Real net national income' : {'batch' : "naag_ch2",
                                                            'measure' : "B5N_R",
                                                            'unit' : 'Index (2020)',
                                                            'link' : "https://www.oecd.org/en/data/indicators/net-national-income.html",
                                                            'link_name' : 'Net national income',
                                                            'test_file': "OECD - income data.csv"},
                     'Expenditure: Final consumption expenditure per capita of households and NPISH' : {'batch' : "naag_ch3",
                                                            'measure' : "P3S1M_POP",
                                                            'unit' : 'Percentage of PPP converted final consumption expenditure per capita of OECD',
                                                            'link' : "https://www.google.com/search?q=Final+consumption+expenditure+per+capita+of+households+and+NPISH&sca_esv=7466096905fafdc9&sxsrf=AE3TifNi0e3a6PCQsM0zqf4BLScWBIQ9BQ%3A1754901838535&ei=Tq2ZaJS2IOa2hbIPi86F-A0&ved=0ahUKEwiUlc7lroKPAxVmW0EAHQtnAd8Q4dUDCBA&uact=5&oq=Final+consumption+expenditure+per+capita+of+households+and+NPISH&gs_lp=Egxnd3Mtd2l6LXNlcnAiQEZpbmFsIGNvbnN1bXB0aW9uIGV4cGVuZGl0dXJlIHBlciBjYXBpdGEgb2YgaG91c2Vob2xkcyBhbmQgTlBJU0gyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEcyChAAGLADGNYEGEdIzQVQAFgAcAF4AJABAJgBXKABXKoBATG4AQPIAQCYAgGgAgOYAwCIBgGQBgiSBwExoAe8B7IHALgHAMIHAzAuMcgHAg&sclient=gws-wiz-serp",
                                                            'link_name' : 'Final consumption expenditure per capita of households and NPISH'},
                     'Expenditure: Individual consumption expenditure of general government' : {'batch' : "naag_ch3",
                                                            'measure' : "P31S13",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : "https://www.oecd.org/en/data/indicators/general-government-spending-by-destination.html",
                                                            'link_name' : 'General government spending by destination'},
                     'Expenditure: Collective consumption expenditure of general government' : {'batch' : "naag_ch3",
                                                            'measure' : "P32S13",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : "https://www.oecd.org/en/data/indicators/general-government-spending-by-destination.html",
                                                            'link_name' : 'General government spending by destination'},
                     'Production: Gross value added growth rate (Total)' : {'batch' : "naag_ch4",
                                                            'measure' : "B1G_GR",
                                                            'activity' : "_T",
                                                            'unit' : 'Percentage change',
                                                            'link' : "https://www.oecd.org/en/data/indicators/value-added-by-activity.html",
                                                            'link_name' : 'Value added by activity'},
                     'Production: Gross value added growth rate (Manufacturing only)' : {'batch' : "naag_ch4",
                                                            'measure' : "B1G_GR",
                                                            'activity' : "C",
                                                            'unit' : 'Percentage change',
                                                            'link' : "https://www.oecd.org/en/data/indicators/value-added-by-activity.html",
                                                            'link_name' : 'Value added by activity'},
                     'Households: Gross disposable income per capita of households and NPISH' : {'batch' : "naag_ch5",
                                                            'measure' : "B6GS1M_POP",
                                                            'unit' : 'US dollars per person, PPP converted',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/household-disposable-income.html',
                                                            'link_name' : 'Household disposable income'},
                     'Households: Debt of households and NPISH' : {'batch' : "naag_ch5",
                                                            'measure' : "LES1M_FD4",
                                                            'unit' : 'Percentage of household and NPISH net disposable income, Current prices',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/household-debt.html',
                                                            'link_name' : 'Household debt'},
                     'Households: Net saving of households and NPISH' : {'batch' : "naag_ch5",
                                                            'measure' : "B8NS1M",
                                                            'unit' : 'Percentage of household and NPISH net disposable income, Current prices',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/household-savings.html',
                                                            'link_name' : 'Household savings'},
                     'Government: Expenditure of general government' : {'batch' : "naag_ch6",
                                                            'measure' : "OTES13",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/general-government-spending.html',
                                                            'link_name' : 'General government spending'},
                     'Government: Revenue of general government' : {'batch' : "naag_ch6",
                                                            'measure' : "OTRS13",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/general-government-revenue.html',
                                                            'link_name' : 'General government revenue'},
                     'Government: Total tax revenue of general government' : {'batch' : "naag_ch6",
                                                            'measure' : "ODAS13_D2_D5_D91_D611_D613",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/tax-revenue.html',
                                                            'link_name' : 'Tax revenue'},
                     'Government: Adjusted debt of general government' : {'batch' : "naag_ch6",
                                                            'measure' : "LES13_FD3",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/general-government-debt.html',
                                                            'link_name' : 'General government debt'},
                     'Government: Net lending(+)/net borrowing(-) of general government' : {'batch' : "naag_ch6",
                                                            'measure' : "B9S13",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : None,
                                                            'link_name' : None},
                     'Government: Expenditure of general government (General public services)' : {'batch' : "naag_ch6a",
                                                            'measure' : "B1GQ_R_POP",
                                                            'expenditure' : "GF01",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : 'https://www.google.com/search?q=oecd+General+public+services&sca_esv=609c72437aa85e53&sxsrf=AE3TifOCqnLS-JmZcQy-_Duw1ksREXwtUw%3A1754937301098&ei=1TeaaNHVBeOp9u8PycjOgQE&ved=0ahUKEwiRlL3zsoOPAxXjlP0HHUmkMxAQ4dUDCBA&uact=5&oq=oecd+General+public+services&gs_lp=Egxnd3Mtd2l6LXNlcnAiHG9lY2QgR2VuZXJhbCBwdWJsaWMgc2VydmljZXMyBRAhGKABMgUQIRigAUigFlDYBVj2EXABeACQAQCYAbYBoAHiBKoBAzAuNLgBA8gBAPgBAZgCBKAC8wPCAgoQABiwAxjWBBhHwgIFEAAY7wXCAggQABiiBBiJBcICCBAAGIAEGKIEwgIFECEYnwWYAwCIBgGQBgiSBwMxLjOgB4wNsgcDMC4zuAfuA8IHBTEuMi4xyAcI&sclient=gws-wiz-serp',
                                                            'link_name' : 'General public services'},
                     'Government: Expenditure of general government (Defense)' : {'batch' : "naag_ch6a",
                                                            'measure' : "B1GQ_R_POP",
                                                            'expenditure' : "GF02",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : None,
                                                            'link_name' : None},
                     'Government: Expenditure of general government (Health)' : {'batch' : "naag_ch6a",
                                                            'measure' : "B1GQ_R_POP",
                                                            'expenditure' : "GF07",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : None,
                                                            'link_name' : None},
                     'Government: Expenditure of general government (Education)' : {'batch' : "naag_ch6a",
                                                            'measure' : "B1GQ_R_POP",
                                                            'expenditure' : "GF09",
                                                            'unit' : 'Percentage of GDP',
                                                            'link' : None,
                                                            'link_name' : None},
                     'Corporations: Debt to gross operating surplus ratio of non-financial corporations' : {'batch' : "naag_ch7",
                                                            'measure' : "LES11_FD4_B2G",
                                                            'unit' : 'Factor of gross operating surplus',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/non-financial-corporations-debt-to-surplus-ratio.html',
                                                            'link_name' : 'Non-financial corporations debt to surplus ratio'},
                     'Corporations: Debt to equity ratio of financial corporations' : {'batch' : "naag_ch7",
                                                            'measure' : "LES12_FD4_F5LI",
                                                            'unit' : 'Factor of equity liabilities',
                                                            'link' : 'https://www.oecd.org/en/data/indicators/financial-corporations-debt-to-equity-ratio.html',
                                                            'link_name' : 'Financial corporations debt to equity ratio'},
                     'Capital: Net capital stock' : {'batch' : "naag_ch8",
                                                            'measure' : "LE_N11N",
                                                            'unit' : 'Index (2020)',
                                                            'link' : 'https://www.google.com/search?q=oecd+Net+capital+stock&oq=oecd+Net+capital+stock&gs_lcrp=EgZjaHJvbWUyBggAEEUYOTINCAEQABiGAxiABBiKBTIHCAIQABjvBTIKCAMQABiABBiiBDIHCAQQABjvBdIBCDU2MzBqMGo0qAIAsAIB&sourceid=chrome&ie=UTF-8',
                                                            'link_name' : 'Net capital stock'},
                     'Employment: Unemployment rate' : {'batch' : "unempl",
                                                            'measure' : "UNE_LF_M",
                                                            'unit' : 'Percentage of labour force',
                                                            'link' : None,
                                                            'link_name' : None},
                     'Trade: Net exports of goods and services contribution to GDP growth' : {'batch' : "naag_ch3a",
                                                            'measure' : "B11_CG",
                                                            'unit' : 'Percentage points',
                                                            'link' : None,
                                                            'link_name' : None}
                    }
        self.euroDict = {
                        'Austria' : 1999,
                        'Belgium' : 1999,
                        'Croatia' : 2023,
                        'Cyprus' : 2008,
                        'Estonia' : 2011,
                        'Finland' : 1999,
                        'France' : 1999,
                        'Germany' : 1999,
                        'Greece' : 2001,
                        'Ireland' : 1999,
                        'Italy' : 1999,
                        'Latvia' : 2014,
                        'Lithuania' : 2015,
                        'Luxembourg' : 1999,
                        'Malta' : 2008,
                        'Netherlands' : 1999,
                        'Portugal' : 1999,
                        'Slovak Republic' : 2009,
                        'Slovenia' : 2007,
                        'Spain' : 1999
                        }

        self.testing = True           

    def updateData(self, new_measure):
        if 'data' in self.dataDict[new_measure].keys():
            return
        batchName = self.dataDict[new_measure]['batch']
        if self.testing:
            url = self.batchDict[batchName]['test_file']
        else:
            url = self.batchDict[batchName]['url']
        batchData = self.getBatch(url).sort_values(by='TIME_PERIOD')
        self.distributeBatch(batchName, batchData)

    def distributeBatch(self, batchName, batchData):
        indicators = [key for key in self.dataDict.keys() if self.dataDict[key]['batch'] == batchName]
        for i in indicators:
            if 'activity' in self.dataDict[i].keys():
                self.dataDict[i]['data'] = batchData[batchData['ACTIVITY'] == self.dataDict[i]['activity']]
            elif 'expenditure' in self.dataDict[i].keys():
                self.dataDict[i]['data'] = batchData[batchData['EXPENDITURE'] == self.dataDict[i]['expenditure']]
            else:
                self.dataDict[i]['data'] = batchData[batchData['MEASURE'] == self.dataDict[i]['measure']]
     
    def getBatch(self, url):
        if self.testing:
            df = pd.read_csv(url)
            return df
        # Fetch data
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for HTTP errors

        # Read CSV data into a pandas DataFrame
        df = pd.read_csv(StringIO(response.text))
        return df

    def getIndicators(self):
        return self.dataDict.keys()

    def getCountries(self, indicator):
        countries = self.dataDict[indicator]['data']['Reference area']
        return sorted(list(set(countries)))

    def getCountryData(self, indicator, countries):
        df = self.dataDict[indicator]['data']
        return df[df['Reference area'].isin(countries)]

    def getMinMaxYear(self, indicator):
        df = self.dataDict[indicator]['data']
        minYear = df['TIME_PERIOD'].unique().min()
        maxYear = df['TIME_PERIOD'].unique().max()
        return minYear, maxYear

    def getUnit(self, indicator):
        return self.dataDict[indicator]['unit']

    def getLink(self, indicator):
        return self.dataDict[indicator]['link']

    def getLinkName(self, indicator):
        return self.dataDict[indicator]['link_name']

    def getEuroYear(self, country):
        return self.euroDict.get(country, None)
        


