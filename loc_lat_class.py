# %%
import pandas as pd
import json
import requests
from pyproj import Proj, transform
from haversine import haversine
import re
# 주소를 입력받아 해당 주소의 위도와 경도를 반환하는 클래스를 정의합니다.
class address_loc_lat():
    def __init__(self):
        # Kakao REST API의 API KEY를 설정합니다.
        self.api_key = "bc889895d6ca2c417542ab48cffaef0a"
        
    # Kakao REST API의 address.json API를 이용하여 위도와 경도 정보를 가져옵니다.
    # 가져온 정보는 pandas의 DataFrame으로 변환하여 반환합니다.
    def use_api(self, addr):
        url = 'https://dapi.kakao.com/v2/local/search/address.json?query={address}'.format(address=addr)
        headers = {"Authorization": "KakaoAK " + self.api_key}
        result = json.loads(str(requests.get(url, headers=headers).text))
        match_first = pd.DataFrame(result['documents'][0]['address'], index=[0])[['address_name', 'x', 'y']]
        match_first = match_first.rename(columns={'x': 'lon', 'y': 'lat'})
        return match_first
        
    # epsg:2097 좌표계로 표현된 org_x, org_y 좌표를 epsg:4326 좌표계로 변환하여 반환합니다.
    def use_bessel(self, org_x, org_y):
        proj_1 = Proj(init='epsg:2097')
        proj_2 = Proj(init='epsg:4326')
        
        x, y = org_x, org_y
        x_, y_ = transform(proj_1, proj_2, x, y)
        
        return x_, y_
    
    # re 모듈을 사용하여 주소를 일부분으로 자르는 함수
    def truncate_address(self,addr):
        match = re.search(r"(.*?(동|리))", addr)
        if match:
            addr = match.group(1)
        
        else:
            addr = addr
        return addr
    
    
    # 두 데이터프레임을 합치고, 거리를 계산하여 추가하는 함수
    def merge_dfs_cal_dist(self, df1, df2):
        df1['key'] = 1
        df2['key'] = 1
        df1 = df1.rename( columns= {'lon':'lon_df1', 'lat':'lat_df1'})
        df1 = df1.rename( columns= {'lon':'lon_df2', 'lat':'lat_df2'})
        merged_df = pd.merge(df1,df2, on = 'keys', how = 'outer', indicator = True)
        merged_df2 = merged_df[
                        (abs(merged_df['lon_df1'] - merged_df['lon_df2']) < 0.05)
                        &
                        (abs(merged_df['lat_df1'] - merged_df['lat_df2']) < 0.06)].drop_duplicates()
        
        merged_df2['distance'] = merged_df2.apply(lambda row: self.dist_cal((row['lon_df1'],row['lat_df1']),(row['lon_df2'],row['lat_df2'])), axis=1)
        
        return merged_df
    # haversine 함수를 사용하여 두 지점 사이의 거리를 계산합니다.
    def dist_cal(loc1,loc2):
        d = round(haversine(loc1,loc2),3)
        return d
        
# %%
