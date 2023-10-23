import pandas as pd

dir = 'data/processedFiles/'

# read in the data from the txt file, columns are user ID, venue ID, venue category ID, venue category name, latitude, longitude, timezone, UTC time
data = pd.read_csv('nycdata/dataset_TSMC2014_NYC.txt', sep='\t', header=None, names=['user_id', 'venue_id', 'venue_category_id', 'venue_category_name', 'latitude', 'longitude', 'timezone', 'UTC_time'], encoding='ISO-8859-1')


# refactor the Venue ID to growing integers and write to file the conversion from old to new
old_venue_id = data['venue_id'].unique()
tuples_venue_id = (old_venue_id, data['venue_id'].astype('category').cat.codes + 1)
venue_id_dict = {}
for i in range(len(tuples_venue_id[0])):
    venue_id_dict[tuples_venue_id[0][i]] = tuples_venue_id[1][i]
with open(dir + 'nyc_poi2index.txt', 'w') as f:
    f.write(str(venue_id_dict))

data['venue_id'] = data['venue_id'].astype('category').cat.codes + 1


# refactor the User ID to growing integers and write to file the conversion from old to new
old_user_id = data['user_id'].unique()
tuples_user_id = (old_user_id, data['user_id'].astype('category').cat.codes + 1)
user_id_dict = {}
for i in range(len(tuples_user_id[0])):
    user_id_dict[tuples_user_id[0][i]] = tuples_user_id[1][i]
with open(dir + 'nyc_user2index.txt', 'w') as f:
    f.write(str(user_id_dict))

data['user_id'] = data['user_id'].astype('category').cat.codes + 1


# write to file the count of unique users
with open(dir + 'nyc_userCount.txt', 'w') as f:
    f.write(str(len(data['user_id'].unique())))

# write to file the count of unique venues
with open(dir + 'nyc_poiCount.txt', 'w') as f:
    f.write(str(len(data['venue_id'].unique())))


# write to file the data in the format: user ID, venue ID, timestamp, latitude, longitude
data['UTC_time'] = pd.to_datetime(data['UTC_time'])

# add column for country code and set to US for all rows
data['Country_code'] = 'US'

#order data by user ID and then timestamp
data = data.sort_values(by=['user_id', 'UTC_time'])

data.to_csv(dir + 'nyc_allData.txt', sep='\t', header=None, index=False, columns=['user_id', 'venue_id', 'UTC_time', 'latitude', 'longitude', 'Country_code'])