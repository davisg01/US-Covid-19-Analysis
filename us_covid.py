import pandas as pd
## import covid data
df = pd.read_csv('covid.csv')

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', None)

## drop fips
df.drop(columns=['fips'], inplace=True)

## dropping territories that are not states
not_states = ['District of Columbia',
    'Puerto Rico',
    'Virgin Islands',
    'Guam',
    'Northern Mariana Islands']
df = df[~df['state'].isin(not_states)]

## dropping irregular counties
not_county = ['Loving', 'Joplin', 'Kansas City',
              'New York City', 'Pending County Assignment']
df = df[~df['county'].isin(not_county)]

## adding month and year as a sepearte column
df['date'] = pd.to_datetime(df['date'])
df['month'] = df['date'].dt.month
df['year'] = df['date'].dt.year

## adding new cases per county as a new column
df['new_cases'] = df.groupby(['county','state'])['cases'].diff().fillna(0)
df['new_cases'] = df['new_cases'].clip(lower=0)

## adding new deaths per county as a new column
df['new_deaths'] = df.groupby(['county','state'])['deaths'].diff().fillna(0)
df['new_deaths'] = df['new_deaths'].clip(lower=0)

## clean df for unknowns and NaN's
df = df[(df['county'] != 'Unknown') & (df.notna().all(axis=1))]

## function for cleaning and reformatting imported population data for all counties
def clean_population_data(file_path):
    idf = pd.read_excel(file_path, skiprows=4)
    idf.drop(columns=idf.columns[1], inplace=True)
    idf.drop(columns=idf.columns[-1], inplace=True)
    idf = idf[:-6]

    idf.columns = ['County', '2020', '2021', '2022']
    idf[['county', 'state']] = idf['County'].str.replace(r'^\.', '', regex=True) \
        .str.replace(' County', '', regex=False) \
        .str.replace(' Parish', '', regex=False) \
        .str.split(', ', expand=True)
    idf.drop(columns=['County'], inplace=True)

    idf = idf[['county', 'state', '2020', '2021', '2022']]
    idf['2020'] = pd.to_numeric(idf['2020'], errors='coerce')
    idf['2021'] = pd.to_numeric(idf['2021'], errors='coerce')
    idf['2022'] = pd.to_numeric(idf['2022'], errors='coerce')
    idf = idf.melt(id_vars=['county', 'state'], var_name='year', value_name='population')

    idf = idf.sort_values(by=['county', 'year']).reset_index(drop=True)
    return idf

## adding county data using function
df_alabama = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\alabama.xlsx")
df_arizona = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\arizona.xlsx")
df_arkansas = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\arkansas.xlsx")
df_california = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\california.xlsx")
df_colorado = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\colorado.xlsx")
df_deleware = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\deleware.xlsx")
df_doc = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\doc.xlsx")
df_florida = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\florida.xlsx")
df_georgia = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\georgia.xlsx")
df_hawaii = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\hawaii.xlsx")
df_idaho = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\idaho.xlsx")
df_illinois = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\illinois.xlsx")
df_indiana = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\indiana.xlsx")
df_iowa = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\iowa.xlsx")
df_kansas = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\kansas.xlsx")
df_kentucky = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\kentucky.xlsx")
df_louisiana = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\louisiana.xlsx")
df_maine = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\maine.xlsx")
df_maryland = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\maryland.xlsx")
df_mass = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\mass.xlsx")
df_michigan = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\michigan.xlsx")
df_minnesota = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\minnesota.xlsx")
df_mississ = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\mississ.xlsx")
df_missouri = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\missouri.xlsx")
df_montana = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\montana.xlsx")
df_nebraska = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\nebraska.xlsx")
df_nevada = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\nevada.xlsx")
df_newham = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\newham.xlsx")
df_newjer = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\newjer.xlsx")
df_newmex = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\newmex.xlsx")
df_newyork = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\newyork.xlsx")
df_northc = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\northc.xlsx")
df_northd = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\northd.xlsx")
df_ohio = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\ohio.xlsx")
df_oklahoma = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\oklahoma.xlsx")
df_oregon = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\oregon.xlsx")
df_penn = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\penn.xlsx")
df_rodeisland = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\rodeisland.xlsx")
df_southc = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\southc.xlsx")
df_southd = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\southd.xlsx")
df_tenn = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\tenn.xlsx")
df_texas = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\texas.xlsx")
df_utah = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\utah.xlsx")
df_vermont = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\vermont.xlsx")
df_virginia = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\virginia.xlsx")
df_washington = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\washington.xlsx")
df_westv = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\westv.xlsx")
df_wisconsin = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\wisconsin.xlsx")
df_wyoming = clean_population_data(r"C:\Users\davis\Documents\sql\state pop\wyoming.xlsx")
df_alaska = pd.read_excel(r"C:\Users\davis\Documents\sql\state pop\alaska.xlsx")
df_alaska['year'] = df_alaska['year'].astype('object')
df_conn = pd.read_excel(r"C:\Users\davis\Documents\sql\state pop\conn.xlsx")
df_conn['year'] = df_conn['year'].astype('object')
df_political = pd.read_excel(r"C:\Users\davis\Documents\sql\state pop\political.xlsx")

## merging all county population data
df_list = [
    df_alabama, df_arizona, df_arkansas, df_california, df_colorado,
    df_deleware, df_doc, df_florida, df_georgia, df_hawaii, df_idaho,
    df_illinois, df_indiana, df_iowa, df_kansas, df_kentucky, df_louisiana,
    df_maine, df_maryland, df_mass, df_michigan, df_minnesota, df_mississ,
    df_missouri, df_montana, df_nebraska, df_nevada, df_newham, df_newjer,
    df_newmex, df_newyork, df_northc, df_northd, df_ohio, df_oklahoma,
    df_oregon, df_penn, df_rodeisland, df_southc, df_southd, df_tenn,
    df_texas, df_utah, df_vermont, df_virginia, df_washington, df_westv,
    df_wisconsin, df_wyoming, df_alaska, df_conn
]

df_pop = pd.concat(df_list, ignore_index=True)
df_pop['population'] = df_pop['population'].astype('int64')
#print(df_pop)

## merging population data with covid data
df['year'] = df['year'].astype(int)
df_pop['year'] = df_pop['year'].astype(int)

## daily change data for counties
df_final = df.merge(df_pop, on=['county','state','year'], how='left')
df_final['cases'] = df_final['cases'].astype(float)
df_final['% total infected'] = (df_final['cases']/df_final['population']).round(5)
df_final['% total deaths'] = (df_final['deaths']/df_final['population']).round(5)
df_final = df_final.sort_values(by=['state','county','date'])
df_final['diff_cases'] = df_final.groupby(['state','county'])['cases'].diff().fillna(0)
df_final['diff_cases'] = df_final['diff_cases'].clip(lower=0)
df_final['% increase infected'] = (df_final['diff_cases']/df_final['cases'].shift(1)).fillna(0)
df_final = df_final.merge(df_political, on=['state'], how='left')

## aggregating daily covid changes to monthly for states
df_final2 = df_final.groupby(['state','year','date','month']).agg({'cases':'sum','deaths':'sum','new_cases':'sum','new_deaths':'sum'}).reset_index()
df_final2 = df_final2.groupby(['state','year','month']).agg({'cases':'last','deaths':'last','new_cases':'sum','new_deaths':'sum'}).reset_index()
df_final_pop = df_pop.groupby(['state','year']).agg({'population':'sum'}).reset_index()

## adding population
df_final2 = df_final2.merge(df_final_pop, on=['state','year'], how='left')

## adding political status
df_final2 = df_final2.merge(df_political, on=['state'], how='left')

##
df_final2['% total infected'] = (df_final2['cases']/df_final2['population']).round(5)
df_final2['% total deaths'] = (df_final2['deaths']/df_final2['population']).round(5)
df_final2 = df_final2.sort_values(by=['state','year','month'])
df_final2['diff_cases'] = df_final2.groupby(['state'])['cases'].diff().fillna(0)
df_final2['diff_cases'] = df_final2['diff_cases'].clip(lower=0)
df_final2['% increase infected'] = (df_final2['diff_cases']/df_final2['cases'].shift(1)).fillna(0)

#print(df_final2.head(300))

## analysis

## ranking the states by most % population infected by covid
query_1 = df_final2.groupby('state')['% total infected'].max().reset_index()
query_1 = query_1.sort_values(by='% total infected', ascending=False)
query_1 = query_1.merge(df_political, on='state', how='left')
print(query_1)

## ranking the counties by most % population infected by covid
query_2 = (
    df_final[['county', 'state', '% total infected']]
    .groupby(['county', 'state'])['% total infected']
    .max()
    .reset_index()
)

df_2022 = df_final[df_final['year'] == 2022][['county', 'state', 'population']].drop_duplicates()
query_2 = query_2.merge(df_2022, on=['county', 'state'], how='left')
query_2 = query_2.sort_values(by='% total infected', ascending=False)
query_2 = query_2[['county', 'state', '% total infected', 'population']]
query_2 = query_2.merge(df_political, on='state', how='left')
query_2_result = query_2.head(100)
print(query_2_result)

## determining what % of the top 100 infected are republican vs democratic
query_3 = (query_2_result['political'].value_counts().get('Republican',0))/len(query_2_result)
print('Republicans represent: ' + str(query_3*100) +'% of the top 100 \n Democrats represent: ' + str(round((1-query_3)*100,2)) + '% of the top 100' )

## ranking the counties by least % population infected
query_4 = query_2.sort_values(by='% total infected', ascending=True)
query_4_result = query_4.head(100)
print(query_4_result)

## determining what % of the bottom 100 infected are republican vs democratic
query_5 = (query_4_result['political'].value_counts().get('Republican',0))/len(query_2_result)
print('Republicans represent: ' + str(query_5*100) +'% of the top 100 \n Democrats represent: ' + str(round((1-query_5)*100,2)) + '% of the top 100' )

## correlation check between population of counties and % total infected
corr_pop_ti = query_2['population'].corr(query_2['% total infected'])
print(corr_pop_ti)
# corr comes out to -0.03 indicating that pop and % infected are not correlated which match the results shown in the
    # top and bot 100 infected counties

## states that had the most infected people
query_6 = df_final2.groupby(['state'])['cases'].max()
query_6 = query_6.sort_values(ascending=False)
print(query_6)

## finding the average % increase for each month for each state
query_7 = df_final2.groupby(['state','month','year']).agg({'% increase infected':'mean'})
## set to below 4 as many counties experience extremely large growth %'s due to its  first initial reports
## thus growth % above were removed as they were seen as outliers
query_7 = query_7[query_7['% increase infected']<=2]
query_7 = query_7.groupby(['state','month']).agg({'% increase infected':'mean'})
print(query_7)

## finding the average % growth of covid for each month in the US
query_8 = query_7.groupby(['month']).agg({'% increase infected':'mean'})
print(query_8)
# results show holidays and summer months to have fairly large growth rates in covid

## which state had the most deaths
query_9 = df_final2.groupby('state').agg({'deaths':'max','% total deaths':'max'})
query_9 = query_9.sort_values('deaths', ascending=False)
print(query_9)
# results show large variations in deaths between states, but looking at population wise they represented below 0.45% of the state population

## determining if there is a difference in growth rates between republican and democratic states
query_10 = df_final2.groupby(['state','month','year','political']).agg({'% increase infected':'mean'})
query_10 = query_10[query_10['% increase infected']<=2]
query_10 = query_10.groupby(['month','political']).agg({'% increase infected':'mean'})
print(query_10)
# found that growth rates are much higher for republicans during the warmer months starting with may until october
# democrats had higher growth rates during months close to winter november until april

## export data for visualization in power bi
df_final.to_csv(r'C:\\Users\davis\Documents\sql\\us_covid_data_bi.csv', index=False)
df_final2.to_csv(r'C:\\Users\davis\Documents\sql\\us_covid_data_month_bi.csv', index=False)
