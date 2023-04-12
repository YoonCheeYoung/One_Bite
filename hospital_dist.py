# %%
import pandas as pd
import multiprocessing as mp
import geopy.distance
from haversine import haversine
import re
def dist_cal(loc1,loc2):
    d = round(haversine(loc1,loc2),3)
    return d




# assuming the column with the addresses is called 'platPlc'
def truncate_address(addr):
    match = re.search(r"(.*?(동|리))", addr)
    if match:
        addr = match.group(1)
       
    else:
        addr = addr
    return addr
# this will remove all sequences of digits followed by "번지"



house_data = pd.read_csv('long_lat_sum_7.csv')
house_loc_data = house_data[['platPlc','x','y']].drop_duplicates()

house_loc_data['add_frac_apt'] =house_data['platPlc'].apply(lambda x: truncate_address(x))

house_loc_data['keys'] = 1

hospital = pd.read_csv('병원.csv')
hospital_data = hospital[['소재지전체주소','lon', 'lat']].dropna()
hospital_data['add_frac_hos'] = hospital_data['소재지전체주소'].apply(lambda x: truncate_address(x))
hospital_data['keys'] = 1

temp = len(house_loc_data) * len(hospital_data)
final = pd.DataFrame()

def merge_df_cal(start, end):

    house_loc_dat = house_loc_data.loc[start:end]
    merged_df = pd.merge(house_loc_dat,hospital_data, on = 'keys', how = 'outer', indicator = True)

    merged_df2 = merged_df[
                        (abs(merged_df['x'] - merged_df['lon']) < 0.05)
                        &
                        (abs(merged_df['y'] - merged_df['lat']) < 0.06)].drop_duplicates()
                        
    merged_df2 = merged_df2[(merged_df['add_frac_apt'] == merged_df['add_frac_hos'])].reset_index()
                        
    merged_df2['distance'] = merged_df2.apply(lambda row: dist_cal((row['y'],row['x']),(row['lat'],row['lon'])), axis=1)
    return merged_df2



chunk_size = 10000
chunk_starts = range(0, temp, chunk_size)
for i, start in enumerate(chunk_starts):
    result = merge_df_cal(start, start+chunk_size)
    filename = f"lat_loc_merged/output_{i}.csv"
    result.to_csv(filename, index=False)

# %%
# if __name__ == '__main__':
#     pool = mp.Pool(processes=2)
#     chunk_size = 100000
#     chunk_starts = range(0, temp, chunk_size)

#     for i, start in enumerate(chunk_starts):
#         print(i)
#         result = pool.apply_async(merge_df_cal, args=(start, start+chunk_size))
#         filename = f"lat_loc_merged/output_{i}.csv"
#         result.get().to_csv(filename, index=False)

# %%
