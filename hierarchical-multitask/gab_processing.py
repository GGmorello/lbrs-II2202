import pandas as pd
import pickle

dir = 'data/processedFiles/'

# read in the data from the txt file, columns are user ID, venue ID, venue category ID, venue category name, latitude, longitude, timezone, UTC time
data = pd.read_csv('nycdata/dataset_TSMC2014_NYC.txt', sep='\t', header=None, names=['USER', 'POI', 'venue_category_id', 'venue_category_name', 'lat', 'lon', 'timezone', 'timestamp'], encoding='ISO-8859-1')
# with open(dir + 'global_scale_allData.pickle', 'rb') as f:
#    data = pickle.load(f)



# data.reset_index(drop=True, inplace=True)


# refactor the Venue ID to growing integers and write to file the conversion from old to new
old_venue_id = data['POI']
tuples_venue_id = (old_venue_id, data['POI'].astype('category').cat.codes + 1)
venue_id_dict = {}
for i in range(len(tuples_venue_id[0])):
    venue_id_dict[tuples_venue_id[0][i]] = tuples_venue_id[1][i]
with open(dir + 'nyc_poi2index.txt', 'w') as f:
    f.write(str(venue_id_dict))

data['POI'] = data['POI'].astype('category').cat.codes + 1

locations = data[['POI', 'lat', 'lon']].drop_duplicates()
# if locations have same venue ID but different latitude and longitude, set latitude and longitude to the first value
locations = locations.groupby(['POI']).first().reset_index()

# use the longitude and latitude from locations to replace the longitude and latitude in data
data = data.merge(locations, on='POI', how='left')

# rename the latitude and longitude columns
data = data.rename(columns={'lat_x': 'lat', 'lon_x': 'lon'})

#remove locations with less than 10 check-ins
data = data.groupby('POI').filter(lambda x: len(x) >= 10)


# refactor the User ID to growing integers and write to file the conversion from old to new
old_user_id = data['USER']
tuples_user_id = (old_user_id, data['USER'].astype('category').cat.codes + 1)
user_id_dict = {}
for i in range(len(tuples_user_id[0])):
    user_id_dict[tuples_user_id[0].iloc[i]] = tuples_user_id[1].iloc[i]
with open(dir + 'nyc_user2index.txt', 'w') as f:
    f.write(str(user_id_dict))

data['USER'] = data['USER'].astype('category').cat.codes + 1


# write to file the count of unique users
with open(dir + 'nyc_userCount.txt', 'w') as f:
    f.write(str(len(data['USER'].unique())))

# write to file the count of unique venues
with open(dir + 'nyc_poiCount.txt', 'w') as f:
    f.write(str(len(data['POI'].unique())))


# write to file the data in the format: user ID, venue ID, timestamp, latitude, longitude
data['timestamp'] = pd.to_datetime(data['timestamp'])

# add column for country code and set to US for all rows
data['Country_code'] = 'US'

#order data by user ID and then timestamp
data = data.sort_values(by=['USER', 'timestamp'])


data.drop(['venue_category_id', 'venue_category_name', 'timezone', 'lat_y', 'lon_y'], axis=1, inplace=True)

# reorder the columns
data = data[['USER', 'POI', 'lat', 'lon', 'timestamp', 'Country_code']]

with open(dir + 'nyc_allData.pickle', 'wb') as f:
    pickle.dump(data, f)
data.to_csv(dir + 'nyc_allData.txt', sep='\t', header=None, index=False, columns=['USER', 'POI', 'timestamp', 'lat', 'lon', 'Country_code'])

# create a file called nyc_train.pkl where we have a list of lists where each list is the check-ins for a user
train = data.groupby('USER')['POI'].apply(list).reset_index(name='POI')
# put the lists in a list
train = train['POI'].tolist()
train = [train, [], []]
print(train)
with open(dir + 'nyc_train.pkl', 'wb') as f:
    pickle.dump(train, f)