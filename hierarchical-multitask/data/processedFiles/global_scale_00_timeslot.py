import pandas as pd
from datetime import datetime

data_source = "global_scale"

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

if __name__ == "__main__":
    prefix = 'nyc_checkin_'
    file = data_source + "_allData.pickle"
    # file = "nycdata/test.tsv"
    columns = ['id','user id','venue id','venue cat id','lat', 'lon', 'offset', 'utc']
    df = pd.read_pickle(file)

    df['date'] = df['timestamp'].apply(reduce_timestamp_to_date)
    df['day'] = df['timestamp'].apply(reduce_timestamp_to_day)
    df['h/m'] = df['timestamp'].apply(lambda t: t.split("T")[1])
    df['day_of_week'] = df['timestamp'].apply(lambda t: datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ').weekday())
    df['slot'] = df[['timestamp', 'day_of_week']].apply(lambda x: reduce_timestamp_to_slot(x[0], x[1]), axis=1)

    grp = df.groupby(['slot'])
    result = grp.agg({'timestamp': ['count']}).sort_values(by="slot")
    print(result)
    # df['slot'] = df[['timestamp','day_of_week']].apply(lambda t: reduce_timestamp_to_slot(t, t))
    # df1 = df.apply(lambda row: row[df['POI'].isin([1,3641])]).sort_values(by=["POI", 'timestamp'])
    # df2 = df1[['POI','date','timestamp','day']]
    # df1 = df.sort_values(by="timestamp")
    #print()
    # print(df)