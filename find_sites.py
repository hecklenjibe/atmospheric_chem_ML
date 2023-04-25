import pandas as pd
import requests
import sys
sys.path.append('/Users/Helen Chen/OneDrive/Documents/HMC/Chem150')
# it says import can't be resolved but it resolves in a notebook? 
from data_fetcher import DataFetcher
import numpy as np
import pandas as pd

PM2_dot_5_VARS = ["PM2.5 - Local Conditions","PM2.5 STP","PM2.5 Total Atmospheric","PM2.5 Raw Data"]

MONITORS_BY_STATE = 'monitors/byState'
ANNUAL_DATA_BY_SITE = 'annualData/bySite'

class findSites():
    """
    Python API to queury from AQS database.
    """

    def __init__(self):
        """
        initializes the site finder
        """
        self.datafetcher = DataFetcher()

    def find_sites(self, param, state, byear, eyear=None):
        """
        Finds sites for a certain parameter within the range of byear to eyear

        Parameters:
            param: String -- name of the parameter we're searching for 
            state: String -- code for the state
            byear: int -- first year we're collecting data for
            eyear: int -- last year we're collecting data for if there is one

        Returns:
            A dataframe with all the sites with valid data for the time range
        """

        df = self.datafetcher.get_data(MONITORS_BY_STATE, self.datafetcher.find_code(param), 20180618, 20180618, df = True, nparams={'state':state})

        if df.empty:
            return df

        # turns start dates into years 
        # df["open_date"] = df["open_date"].map(lambda date: int(str(date)[:4])) 
        # # sorts so that the earliest ozone collection date is before 1980
        # # df = df[df["open_date"] < byear]
        # # df = df.reset_index()

        # # finds data within the correct year range
        # if eyear == None:
        #     df = df.fillna(value='None')
        #     df = df[df["close_date"] == 'None']
        # else:
        #     df = df.fillna(value="9999")
        #     df["close_date"] = df["close_date"].map(lambda date: int(str(date)[:4])) 
        #     df = df[df["close_date"] > eyear]
        #     df = df.replace(to_replace=9999, value='None') 

        df = df.drop_duplicates(subset = "site_number")
        df = df.reset_index()

        return df[['site_number', 'local_site_name', 'county_code', 'latitude','longitude', 'open_date', 'close_date']]
    
    def best_sites_state(self, state, byear, eyear=None, mandatory_param='PM2.5 - Local Conditions', verbose=False):
        """
        Finds all sites with potential data for PM2.5

        Parameters:
            state: String -- state code
            byear: int -- first year
            eyear: int -- last year (if there is one)
            mandatory_params: String -- parameter searched for

        Returns:
            A dataframe with all valid sites, or an empty dataframe if there are no matching sites
            These sites have the mandatory parameter (with hourly check??)
        """

        dfs = self.find_sites(mandatory_param, state, byear, eyear)
        if dfs.empty:
            print(f"No matching sites found for state {state}")
            return dfs
        found_params = [mandatory_param]

        # # starts dictionary for the aggregation function later
        # param_dict = {mandatory_param : 'sum'}

        # # aggregates all data together
        # mini_func = {'local_site_name': 'first', 'county_code': 'first'}
        # aggregation_functions = {**mini_func}#, **param_dict}
        # dfs = dfs.groupby(dfs['site_number']).aggregate(aggregation_functions)
        dfs.set_index('site_number',inplace=True)

        # makes sure mandatory variable is hourly
        for param in [mandatory_param]:
            for index, row in dfs.iterrows():
                bdate = str(int(byear)) + '0101'
                annual_df = self.datafetcher.get_data(ANNUAL_DATA_BY_SITE, self.datafetcher.find_code(param), bdate, bdate, df = True, nparams={'state':state, 'county':row[1], 'site':index})
                # if there is no annual data at all
                if annual_df.empty:
                    dfs.drop(labels=[index], axis=0, inplace=True)
                    continue
                annual_df = annual_df[annual_df['sample_duration'] == '1 HOUR']
                if annual_df.empty:
                    dfs.drop(labels=[index], axis=0, inplace=True)
                else:
                    pass
                    # dfs.insert(-1, '')
            
            if dfs.empty:
                print(f"No hourly data found for state {state} for mandatory parameter {param}")
                # returns an empty dataframe 
                return dfs

        dfs.sort_values(by='open_date',inplace=True)
        dfs.reset_index(inplace=True)

        return dfs


    def best_sites_country(self, byear, eyear=None, mandatory_param='PM2.5 - Local Conditions'):
        """
        Finds the best sites in the country given a year range and parameters

        Parameters:
            state: String -- state code
            byear: int -- first year
            eyear: int -- last year (if there is one)
            mandatory_params: String -- desired parameter

        Returns:
            A dataframe with all valid sites, or an empty dataframe if there are no matching sites
            These sites have the mandatory parameter
            The parameter must have hourly data to show up 
        """
        states = self.get_state_codes()
        # states = states.loc[[0, 1, 2, 3]]

        dfs = pd.DataFrame()
        for index, row in states.iterrows():
            df =  self.best_sites_state(str(row['state_code']), byear, eyear, mandatory_param)

            if not df.empty:
                # weeds out anything with less than 6 features 
                # df = df[df['total_params'] >= 6]

                # adds state identifying information
                df.insert(0, 'state_name', row['state_name'])
                df.insert(0, 'state_code', row['state_code'])
                df.insert(0, 'climate_zone', row['climate_zone'])

                dfs = pd.concat([dfs, df], axis=0)

            print(f"Finished state {row['state_name']}")

        dfs.fillna(0, inplace=True)

        return dfs

    def get_state_codes(self):
        """
        Returns a dataframe of the state codes for easy reference outside the site finder

        Returns:
            Dataframe!
        """
        url = "https://aqs.epa.gov/data/api/list/states"
        r = requests.get(url=url,params={'email':'hechen@g.hmc.edu','key':'saffronwren65'})
        print(f"{r}")
        data = r.json()['Data']
        df = pd.DataFrame(data)

        df = df.rename({'code': 'state_code'}, axis=1)
        df = df.rename({'value_represented': 'state_name'}, axis=1)
        # drops all non-states
        df = df.drop(df.index[[51,52,53,54,55]])

        # adds climate zone
        for index, row in df.iterrows():
            df.at[index,'climate_zone'] = CLIMATE_ZONES[row['state_name']]

        df.reset_index(inplace = True)

        return df

    # def search_usa(self, year):
    #     """
    #     Returns a dataframe that searches the U.S.A. for good sites


    #     """
    #     # gets the states 
    #     r = requests.get(url='https://aqs.epa.gov/data/api/list/states?email=orussell@g.hmc.edu@aqs.api&key=silverwren95')

    #     row = df_08_oz_00.iloc[[0,1,2,3]].copy()
    #     row['state_number'] = '08'
    #     row['state_name'] = 'Colorado'
    #     row['climate_zone'] = 'Southwest'
    #     df_ozone_2000 = df_ozone_2000.append(row)

CLIMATE_ZONES = {
    'Washington' : 'Northwest',
    'Oregon' : 'Northwest',
    'Idaho' : 'Northwest',
    'California' : 'West',
    'Nevada' : 'West',
    'Utah' : 'Southwest',
    'Colorado' : 'Southwest',
    'Arizona' : 'Southwest',
    'New Mexico' : 'Southwest',
    'Montana' : 'Northern Rockies and Plains',
    'North Dakota' : 'Northern Rockies and Plains',
    'South Dakota' : 'Northern Rockies and Plains',
    'Wyoming' : 'Northern Rockies and Plains',
    'Nebraska' : 'Northern Rockies and Plains',
    'Minnesota' : 'Upper Midwest',
    'Iowa' : 'Upper Midwest',
    'Wisconsin' : 'Upper Midwest',
    'Michigan' : 'Upper Midwest',
    'Kansas' : 'South',
    'Oklahoma' : 'South',
    'Texas' : 'South',
    'Louisiana' : 'South',
    'Arkansas' : 'South',
    'Mississippi' : 'South',
    'Hawaii' : 'N/A',
    'Alaska' : 'N/A',
    'Illinois' : 'Ohio Valley',
    'Missouri' : 'Ohio Valley',
    'Tennessee' : 'Ohio Valley',
    'Kentucky' : 'Ohio Valley',
    'Ohio' : 'Ohio Valley',
    'Indiana' : 'Ohio Valley',
    'West Virginia' : 'Ohio Valley',
    'Alabama' : 'Southeast',
    'Georgia' : 'Southeast',
    'South Carolina' : 'Southeast',
    'North Carolina' : 'Southeast',
    'Virginia' : 'Southeast',
    'District Of Columbia' : 'Southeast',
    'Florida' : 'Southeast',
    'Maryland' : 'Northeast',
    'Pennsylvania' : 'Northeast',
    'Delaware' : 'Northeast', 
    'New Jersey' : 'Northeast',
    'Connecticut' : 'Northeast',
    'Rhode Island' : 'Northeast',
    'Massachusetts' : 'Northeast',
    'Vermont' : 'Northeast',
    'New Hampshire' : 'Northeast',
    'New York' : 'Northeast',
    'Maine' : 'Northeast'
}