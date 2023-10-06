import pandas as pd

if __name__ == '__main__': 
    prefix = 'nyc_checkin_'
    file = "nycdata/dataset_TSMC2014_NYC copy.txt"
    # file = "nycdata/test.tsv"
    columns = ['id','user id','venue id','venue cat id','lat', 'lon', 'offset', 'utc']
    df = pd.read_table(file, header=None, names=columns)

    for index, row in df.iterrows():
        a = 123
    print(df)

    uq = df['id'].unique()
    uq_ven = df['venue id'].unique()
    uq_user = df['user id'].unique()
    print("uq id " + str(len(uq)))
    print("uq ven " + str(len(uq_ven)))
    print("uq_user " + str(len(uq_user)))

