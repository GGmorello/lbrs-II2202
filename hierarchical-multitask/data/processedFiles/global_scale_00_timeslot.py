import pandas as pd
from datetime import datetime

data_source = "nyc"

prefix = "_test"

def reduce_timestamp_to_day(t):
    return t.split("-")[2].split("T")[0]

def reduce_timestamp_to_date(t):
    s = t.split("-")
    return s[0] + "-" + s[1]

def reduce_timestamp_to_slot(ts, dow):
    slot = 0
    if (dow > 0):
        slot = dow*8
    else:
        slot = 0
    
    s = ts.split("T")[1].split(":")
    h = int(s[0])
    h = h - (h % 3)

    slot += int(h/3)

    return "" + str(slot)

def compute_jaccard(slots1, slots2):
    limit = 0.9
    union = list(set(slots1.union(slots2)))
    intersect = list(set(slots1) & set(slots2))
    return (len(intersect)* 1.0)/(len(union)* 1.0) >= limit

def create_temporal_dump(df):
    pois_dict = {}  # Use a set instead of list to eliminate duplicates
    slots_dict = {}

    temporal_graph_curr_fname = data_source + "_temporal" + prefix + ".edgelist"
    f_temporal = open(temporal_graph_curr_fname, 'w')

    for _, row in df.iterrows():
        poi = row['POI']
        slot = row['slot']

        if poi not in pois_dict:
            pois_dict[poi] = set()
        pois_dict[poi].add(slot)

        if slot not in slots_dict:
            slots_dict[slot] = set()
        slots_dict[slot].add(poi)

    computed_pois = set()
    for poi, slots in pois_dict.items():
        print("processing key", poi)
        for slot in slots:
            other_pois = slots_dict[slot]  # Use set difference to exclude current poi
            for other_poi in other_pois:
                if (poi, other_poi) in computed_pois or (other_poi, poi) in computed_pois:
                    continue

                other_slots = pois_dict[other_poi]

                if compute_jaccard(slots, other_slots):
                    e = str(poi) + " " + str(other_poi) + " {}\n"
                    print("adding edge to file", e)
                    f_temporal.write(e)
                
                computed_pois.add((poi, other_poi))
                computed_pois.add((other_poi, poi))
    print("dumped temporal graph in file", temporal_graph_curr_fname)

# Your compute_jaccard function remains unchanged

if __name__ == "__main__":
    file = data_source + "_allData.pickle"
    if data_source == "nyc":
        prefix = ""
    # file = "nycdata/test.tsv"
    columns = ['id','user id','venue id','venue cat id','lat', 'lon', 'offset', 'utc']
    df = pd.read_pickle(file)

    df['date'] = df['timestamp'].apply(reduce_timestamp_to_date)
    df['day'] = df['timestamp'].apply(reduce_timestamp_to_day)
    df['h/m'] = df['timestamp'].apply(lambda t: t.split("T")[1])
    df['day_of_week'] = df['timestamp'].apply(lambda t: datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ').weekday())
    df['slot'] = df[['timestamp', 'day_of_week']].apply(lambda x: reduce_timestamp_to_slot(x[0], x[1]), axis=1)
    # grp = df.groupby(['slot'])
    # result = grp.agg({'timestamp': ['count']}).sort_values(by="poi")
    # print(result)
    # df['slot'] = df[['timestamp','day_of_week']].apply(lambda t: reduce_timestamp_to_slot(t, t))
    # df1 = df.apply(lambda row: row[df['POI'].isin([1,3641])]).sort_values(by=["POI", 'timestamp'])
    # df2 = df1[['POI','date','timestamp','day']]
    # df1 = df.sort_values(by="timestamp")
    #print()
    create_temporal_dump(df)