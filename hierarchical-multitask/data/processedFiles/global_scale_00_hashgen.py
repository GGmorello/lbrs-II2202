import pickle
import pandas as pd
import numpy as np
import pygeohash as pgh

data_source = "nyc"
postfix = "_test"

all_data_file = data_source + '_allData.pickle'

def get_poi2geo_file_name(hash_len):
    base_name = data_source + '_poi2geohash' + prefix
    return base_name + '_' + str(hash_len) + '.pickle'

def get_geo2poi_file_name(hash_len):
    base_name = data_source + '_geohash2poi' + prefix
    return base_name + '_' + str(hash_len) + '.pickle'

def get_geohash2index_file_name(hash_len):
    base_name = data_source + '_geohash2Index' + prefix
    return base_name + '_' + str(hash_len) + '.pickle'

def get_beam_file_name():
    return data_source + '_beamSearchHashDict' + prefix + '.pickle'

def create_poi2geo_dump(i, copy):
    copy[0] = 0
    fname_poi2geo = get_poi2geo_file_name(i)
    f_poi2geo = open(fname_poi2geo, 'wb')
    pickle.dump(copy, f_poi2geo)
    print("dumped poi2geo file contents in path: ", fname_poi2geo)

def create_geo2other_dumps(i, copy):
    geohash_index = 0
    hash2poi = dict()
    geohash_idx_dict = dict()
    for item in copy.items():
        poi = item[0]
        hash = item[1]
        val = hash[0:i]

        if val not in geohash_idx_dict:
            geohash_index += 1
            geohash_idx_dict[val] = geohash_index

        if hash2poi.get(val) == None:
            hash2poi[val] = list()
        hash2poi[val].append(poi)
    hash2poi[0] = []
    fname_geo2poi = get_geo2poi_file_name(i)
    f_geo2poi = open(fname_geo2poi, 'wb')
    pickle.dump(hash2poi, f_geo2poi)
    print("dumped geo2poi file contents in path: " , fname_geo2poi)

    geohash_idx_dict[0] = 0
    fname_geohash_idx = get_geohash2index_file_name(i)
    f_geohash_idx = open(fname_geohash_idx, 'wb')
    pickle.dump(geohash_idx_dict, f_geohash_idx)
    print("dumped geohash2Index file contents in path: ", fname_geohash_idx)

def create_beam_dump(content):
    beam_dict = dict()
    for val in content.values():
        for i in range (2, 6):
            j = i + 1
            k = str(i) + "_" + str(j)
            if k not in beam_dict:
                beam_dict[k] = dict()
            sub0 = val[0:i]
            sub1 = val[0:j]
            if sub0 not in beam_dict[k]:
                beam_dict[k][sub0] = []
            if sub1 not in beam_dict[k][sub0]:
                beam_dict[k][sub0].append(sub1)
    
    fname_beam = get_beam_file_name()
    f_beam = open(fname_beam, 'wb')
    pickle.dump(beam_dict, f_beam)
    print("dumped beamdict file contents in path: " , fname_beam)

def create_dumps(content):
    create_beam_dump(content)
    for i in range (2,7,1):
        copy = dict(map(lambda item: (item[0], item[1][0:i]), content.items()))
        create_poi2geo_dump(i, copy)
        copy = dict(map(lambda item: (item[0], item[1][0:i]), content.items()))
        create_geo2other_dumps(i, copy)

def verify_poi2geohash(i):
    fname = data_source + '_poi2geohash_' + str(i) + '.pickle'
    orig = open(fname, 'rb')
    orig_dict = pickle.load(orig)
    
    fname_curr = get_poi2geo_file_name(i)
    curr = open(fname_curr, 'rb')
    curr_dict = pickle.load(curr)

    for k in dict(orig_dict).keys():
        # TODO: Why is the 0 entry included in the original data?
        if k == 0:
            continue
        orig_val = orig_dict[k]
        curr_val = curr_dict[k][0:i]
        if orig_val != curr_val:
            print("values differ for key " + str(k) + " - orig: " + str(orig_val) + ", curr: " + str(curr_val))
    print("verified content in file:", fname_curr)

def verify_geohash2poi(i):
    fname = data_source + '_geohash2poi_' + str(i) + '.pickle'
    orig = open(fname, 'rb')
    orig_dict = pickle.load(orig)

    fname_curr = get_geo2poi_file_name(i)
    curr = open(fname_curr, 'rb')
    curr_dict = pickle.load(curr)

    for k in orig_dict.keys():
        # TODO: Why is the 0 entry included in the original data?
        if k == 0:
            continue
        curr_vals = curr_dict[k]
        orig_vals = orig_dict[k]
        if len(curr_vals) != len(orig_vals):
            print("length of arrs for key " + str(k) + " differ", orig_vals, curr_vals)
        for poi in orig_dict[k]:
            if not poi in curr_vals:
                print('missing value ' + str(poi) +' for key ' + str(k) + ' in curr vals', curr_vals)
    print("verified content in file:", fname_curr)

def verify_geohash2Index(i):
    fname = data_source + '_geohash2Index_' + str(i) + '.pickle'
    orig = open(fname, 'rb')
    orig_dict = pickle.load(orig)

    fname_curr = get_geohash2index_file_name(i)
    curr = open(fname_curr, 'rb')
    curr_dict = pickle.load(curr)

    for k in dict(orig_dict).keys():
        # TODO: Why is the 0 entry included in the original data?
        if k == 0:
            continue
        curr_val = curr_dict[k]
        orig_val = orig_dict[k]
        if curr_val != orig_val:
            print('invalid global idx value for key ' + str(k) + ', orig: ' + str(orig_val) + ", curr: " + str(curr_val))
    print("verified content in file:", fname_curr)

def verify_beam():
    fname = data_source + '_beamSearchHashDict.pickle'
    orig = open(fname, 'rb')
    orig_dict = pickle.load(orig)

    fname_curr = get_beam_file_name()
    curr = open(fname_curr, 'rb')
    curr_dict = pickle.load(curr)

    for k in dict(orig_dict).keys():
        curr_vals = curr_dict[k]
        orig_vals = orig_dict[k]
        if len(curr_vals) != len(orig_vals):
            print("length of orig/curr beam differ for key " + str(k), orig_vals, curr_vals)
        for orig_val in orig_vals:
            if not orig_val in curr_vals:
                print("missing value in curr vals", orig_val, curr_vals)
    print("verified content in file:", fname_curr)

def verify():
    verify_beam()
    for i in range (2, 7):
        verify_poi2geohash(i)
        verify_geohash2poi(i)
        verify_geohash2Index(i)

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
    if data_source == "nyc":
        prefix = ""
    create_dumps(content)
    if data_source != "nyc":
        verify()
