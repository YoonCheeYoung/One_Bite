# %%
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


tmp_merged = pd.DataFrame()

f_num = 0
for i, address in enumerate(dejang):
    params = {"query": f"{address}"}


    response = requests.get(url, params=params, headers=headers)

    tmp = pd.DataFrame(response.json())
    
    tmp_merged = pd.concat([tmp_merged, tmp])
    
    if i//10000 == 0:
        tmp_merged.to_csv(f'위도경도_{f_num}.csv')
        tmp_merged = pd.DataFrame()
        f_num = f_num + 1
    
# %%
