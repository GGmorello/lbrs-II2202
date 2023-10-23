import pickle
import pandas as pd
import numpy as np
import pygeohash as pgh

data_source = "global_scale"

all_data_file = data_source + '_allData.pickle'

def get_file_name(hash_len):
    base_name = data_source + '_poi2geohash_test'
    return base_name + '_' + str(hash_len) + '.pickle'

def create_dumps(content):
    copy = content.copy()
    for i in range (6,1,-1):
        if i != 6:
            copy = dict(map(lambda item: (item[0], item[1][0:i]), copy.items()))
        fname = get_file_name(i)
        f = open(fname, 'wb')
        pickle.dump(content, f)
        print("dumped file contents in path:",fname)

def verify(content):
    for i in range (2, 7):
        fname = data_source + '_poi2geohash_' + str(i) + '.pickle'
        orig = open(fname, 'rb')
        orig_dict = pickle.load(orig)
        for k in dict(orig_dict).keys():
            if k == 0:
                continue
            orig_val = orig_dict[k]
            curr_val = content[k][0:i]
            if orig_val != curr_val:
                print("values differ for key " + str(k) + " - orig: " + str(orig_val) + ", curr: " + str(curr_val))
        print("verified content to file:",fname)

if __name__ == "__main__":
    # key: POI, value: full hash
    content = dict()

    cols = ['id','user idx','poi idx','ts','lat', 'lon', 'cc']
    df = pd.read_pickle(all_data_file)

    print("len of rows in allData", len(df), all_data_file)

    grp = df.groupby(['POI', 'lat', 'lon'])
    result = grp.agg({'timestamp': ['count']})
    print("result len", len(result))
    print("sum of rows", result.sum())

    for group_name, df_group in grp:
        poi = group_name[0]
        lat = group_name[1]
        lon = group_name[2]
        geohash = pgh.encode(latitude=lat, longitude=lon)
        content[poi] = geohash[0:6]

    create_dumps(content)
    verify(content)
