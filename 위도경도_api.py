# %%
'''
import pandas as pd
import requests

dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
dejang = dejang['newPlatPlc'].drop_duplicates()
dejang = dejang[:10]

url = "https://dapi.kakao.com/v2/local/search/address.json"

api_key = "bc889895d6ca2c417542ab48cffaef0a"
headers = {
    "Authorization": "KakaoAK " + api_key
}



# %%
dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
dejang = dejang['newPlatPlc'].drop_duplicates()

for f_num in [*range(0, len(dejang)//10000)]: 
    n = f_num*10000
    dejang_work = dejang[n:n+10001]

    tmp_merged = pd.DataFrame()

    for address in dejang_work:
        params = {"query": f"{address}"}
        try:
            response = requests.get(url, params=params, headers=headers)

            tmp = pd.DataFrame(response.json()['documents'])[['address_name','x','y']]
        except:
            pass
        tmp_merged = pd.concat([tmp_merged, tmp])
    tmp_merged.to_csv(f'data/smaple{f_num}.csv', encoding = 'euc-kr')


# %%
import pandas as pd

df = pd.read_csv('C:/Users/mhcho/코리아리서치인터내셔널/AI Big Data 센터 - General/data_test/sample0.csv')


df = pd.read_csv('C:/Users/mhcho/코리아리서치인터내셔널/AI Big Data 센터 - General/data_test/smaple0.csv', encoding = 'cp949')
# %%

import os
import pandas as pd


dir = 'data'

files = [f for f in os.listdir(dir) if f.endswith('.csv')]

for file in files:
    file_path = os.path.join(dir, file)
    df = pd.read_csv(file_path, encoding = 'cp949')
    dd = df[df['address_name'] == '경북 포항시 남구 연일읍 연일로 165-3']
    if len(dd) !=0:
        print(file)

        
# %%

check = pd.read_csv('data/smaple9.csv',encoding = 'cp949')
check.reset_index(inplace = True, drop = True)
check_index = check[check['address_name'] == '경북 포항시 남구 연일읍 연일로 165-3']
print(check_index)
# %%
      
dejang_flag = dejang['경북 포항시 남구 연일읍 연일로 165-3':]
# %%
dejang_flag = dejang[dejang == '경북 포항시 남구 연일읍 연일로 165-3']

# Get all values after the specific value
dejang2 = dejang.loc[dejang_flag + 1:]
# %%
import pandas as pd
import requests

dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
# %%


import dask.dataframe as dd

sample = dd.read_csv('data/merged_data_0.csv')


# %%


url = "https://dapi.kakao.com/v2/local/search/address.json"

api_key = "bc889895d6ca2c417542ab48cffaef0a"
headers = {
    "Authorization": "KakaoAK " + api_key
}



def api_address(df):
    tmp_merged = pd.DataFrame()
    address_series = df['newPlatPlc'].drop_duplicates()
    for address in address_series:
            params = {"query": f"{address}",
                      "analyze_type":"exact"}
            try:
                response = requests.get(url, params=params, headers=headers)

                tmp = pd.DataFrame(response.json()['documents'])[['address_name','x','y']]
            except:
                tmp = pd.DataFrame()
            
            tmp_df = df[df['newPlatPlc'] ==address].reset_index(drop = True)
            tmp_df = pd.merge(tmp_df, tmp, left_index = True, right_index = True)
            tmp_merged = pd.concat([tmp_merged, tmp_df])

    return tmp_merged
# %%
dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
dejang['newPlatPlc'] = dejang['newPlatPlc'].str.lstrip()
dejang = dejang.sort_values(by = 'newPlatPlc')
dejang= dejang.drop(['Unnamed: 0'], axis = 1).reset_index()
# %%
sample = dejang[:10]

test_sample = api_address(sample)
# %%
test_sample[test_sample['newPlatPlc']=='강원도 강릉시 종합운동장길 92']
# %%
df = sample

tmp_merged = pd.DataFrame()
address_series = df['newPlatPlc'].drop_duplicates()
for address in address_series:
        params = {"query": f"{address}",
                    "analyze_type":"exact"}
        
        
        response = requests.get(url, params=params, headers=headers)

        tmp = pd.DataFrame(response.json()['documents'])[['address_name','x','y']]

        
        tmp_df = df[df['newPlatPlc'] ==address].reset_index(drop = True)
        tmp_df = pd.merge(tmp_df, tmp, left_index = True, right_index = True)
        tmp_merged = pd.concat([tmp_merged, tmp_df])
# %%
import requests
import json

dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
dejang['newPlatPlc'] = dejang['newPlatPlc'].str.lstrip()
dejang = dejang.sort_values(by = 'newPlatPlc')
dejang= dejang.drop(['Unnamed: 0'], axis = 1).reset_index()


api_key = "bc889895d6ca2c417542ab48cffaef0a"

def addr_to_lat_lon(addr):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
    headers = {"Authorization": "KakaoAK " + api_key}
    result = json.loads(str(requests.get(url, headers=headers).text))
    match_first = pd.DataFrame(result['documents'][0]['address'], index = [0])[['address_name','x','y']]
    return match_first


def call_concat(df):
    test_sample = df.apply(lambda x: addr_to_lat_lon(x['platPlc']), axis = 1)

    raw_part = sample[sample['platPlc'] == addr]
    raw_part = pd.concat([raw_part,match_first], axis = 1)
sample = dejang[:10]

test_sample = sample.apply(lambda x: addr_to_lat_lon(x['platPlc']), axis = 1)
# %%
def addr_to_lat_lon(addr):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
    headers = {"Authorization": "KakaoAK " + api_key}
    result = json.loads(str(requests.get(url, headers=headers).text))
    match_first = pd.DataFrame(result['documents'][0]['address'], index = [0])[['address_name','x','y']]
    return match_first

test_sample = sample.apply(lambda x: addr_to_lat_lon(x['platPlc']), axis = 1)
'''
# %%

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas

def addr_to_lat_lon(addr):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
    headers = {"Authorization": "KakaoAK " + api_key}
    result = json.loads(str(requests.get(url, headers=headers).text))
    match_first = pd.DataFrame(result['documents'][0]['address'], index = [0])[['address_name','x','y']]
    return match_first



dejang = pd.read_csv('건축물대장_전처리.csv', encoding = 'cp949')
dejang['newPlatPlc'] = dejang['newPlatPlc'].str.lstrip()
dejang = dejang.sort_values(by = 'newPlatPlc')
dejang= dejang.drop(['Unnamed: 0'], axis = 1).reset_index()

address_series_whole = dejang['platPlc'].drop_duplicates()
tmp_merged = pd.DataFrame()

for f_num in range(*[0,len(address_series_whole)//10000]):
    n = f_num*10000
    address_series = address_series_whole[n:n+10000]
    for addr in address_series:
        try:
            long_lat = addr_to_lat_lon(addr)
        except:
            long_lat = pd.DataFrame({'address_name':[0], 'x':[0],'y':[0]})
        raw_part = dejang[dejang['platPlc'] == addr]
        raw_part['address_name'] = long_lat.iloc[0,0]
        raw_part['x'] = long_lat.iloc[0,1]
        raw_part['y'] = long_lat.iloc[0,2]
        tmp_merged = pd.concat([tmp_merged,raw_part])
    tmp_merged.to_csv(f'data/long_lat_data_{f_num}.csv')
    
# %%

# %%

from haversine import haversine
import pandas as pd

test_sample = pd.read_csv('data/long_lat_data.csv')



def add2longlat(loc1, loc2):
  '''
  loc1 : (latitude, longtitude) tuple of location_1
  loc2 : (latitude, longtitude) tuple of location_2
  return : distance btw loc1-loc2 in km
  '''
  return(round(haversine(loc1, loc2), 3))



def facil_dist(org, df_name, col_name):
    org = pd.read_csv('근린/long_lat/{df_name}.csv')
    test_sample[['{col_name}_500m','{col_name}_1km','{col_name}_2km']] = 0,0,0
    for i in range(len(test_sample)):
        temp = facil[facil['add_fac'] == test_sample.loc[i,'add_fac']].reset_index(drop=True)
        # count_500= 0
        # count_1000= 0
        # count_2000= 0
        for j,row in temp.iterrows():
            dist = add2longlat((df_test_samplename.loc[i,'add.lat'],apt_df.loc[i,'add.lon']), (row['add.lat'],row['add.lon']))
            # print(dist)
            if dist <= 0.5: org.loc[i,'{col_name}__500m'] += 1
            if dist <= 1.0: org.loc[i,'{col_name}__1km'] += 1
            if dist <= 2.0: org.loc[i,'{col_name}__2km'] += 1
        if i % 100 == 0:
            print(f'park {i} rows completed')
    return test_sample

test_sample_added = facil_dist('병원','병원')


# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Define a function to find the minimum distance to a point in table2 for each point in table1
def find_nearest_distance(point):
    # Use the spatial index to find the nearest point in table2
    nearest_point_idx = list(sindex.nearest(point.buffer(0.01).bounds, 1))[0]
    nearest_point = table2.iloc[nearest_point_idx]
    # Compute the distance between the point and the nearest point in table2
    distance = nearest_point.geometry.distance(point)
    return distance

def count_nearby_points(point, dist):
    # Use the spatial index to find all points within a distance radius
    nearby_points_idx = list(sindex.intersection(point.buffer(0.01).bounds))
    nearby_points = table2.iloc[nearby_points_idx]
    # Compute the distance between the point and each nearby point
    distances = nearby_points.distance(point)
    # Count the number of nearby points within the distance
    count = (distances < dist).sum()
    return count

def facil_add(table1, table2_path, col_name):
    # Read the CSV files into Pandas dataframes
  
    table2 = pd.read_csv(table2_path)

    # Convert the dataframes to Geopandas dataframes by creating Point geometries from the latitude and longitude columns
    table1 = gpd.GeoDataFrame(table1, geometry=gpd.points_from_xy(table1.longitude, table1.latitude))
    table2 = gpd.GeoDataFrame(table2, geometry=gpd.points_from_xy(table2.longitude, table2.latitude))

    # Create spatial index for table2
    sindex = table2.sindex

    # Compute the nearest distance and nearby points for each point in table1
    table1['nearest_distance'] = table1.geometry.apply(find_nearest_distance)
    table1[f'{col_name}_500m'] = table1.geometry.apply(lambda x: count_nearby_points(x, 500))
    table1[f'{col_name}_1km'] = table1.geometry.apply(lambda x: count_nearby_points(x, 1000))
    table1[f'{col_name}_2km'] = table1.geometry.apply(lambda x: count_nearby_points(x, 2000))

    return table1

# %%
table1 = pd.read_csv(table1_path)
table1 = facil_add(test_sample_added, '근린\long_lat\병원.csv')
table1 = facil_add(table1, '근린\long_lat\의원.csv')
table1 = facil_add(table1, '근린\long_lat\지하철.csv')
table1 = facil_add(table1, '근린\long_lat\학교.csv')
table1 = facil_add(table1, '근린\long_lat\대규모점포.csv')
