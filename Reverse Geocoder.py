import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from us_state_abbrev import us_state_to_abbrev
from tqdm import tqdm

def get_state(latitude, longitude):
    no_state_coords = ['41.1, -73.68', '41.7039, -71.8663', '41.7039, -71.8663', '41.3822, -71.8364',
                       '41.9942, -71.898']
    coordinates = str(latitude) + ', ' + str(longitude)
    print(coordinates)

    # Create a Nominatim geocoder object and a rate limiter for geocoding to avoid overloading the service
    locator = Nominatim(user_agent='tornado_state', timeout=10)
    geocode = RateLimiter(locator.reverse, min_delay_seconds=1)
    
    # Return CT if the coordinates are in Connecticut, else run the geocoding
    if coordinates in no_state_coords:
        return 'CT'
    else:
        # Retrieve the state information based on the given coordinates
        state = geocode(coordinates).raw['address']['state']
    
    # Convert the full state name to its abbreviation
    return us_state_to_abbrev[state]

tornado_df = pd.read_csv('./2000-2022_actual_tornadoes.csv')

# Insert new columns for start and end states in the DataFrame
tornado_df.insert(17, 'start_st', 0)
tornado_df.insert(20, 'end_st', 0)

# Iterate over each row in the DataFrame
for i, row in tqdm(tornado_df.iterrows()):
    if row['sn'] == 1:
        # If it is the start of a tornado, set the start and end states to the same value
        tornado_df.at[i, 'start_st'] = row['st']
        tornado_df.at[i, 'end_st'] = row['st']
    else:
        # If it is not the start, retrieve the state for the given latitude and longitude
        tornado_df.at[i, 'start_st'] = get_state(row['slat'], row['slon'])
        tornado_df.at[i, 'end_st'] = get_state(row['elat'], row['elon'])

# Save the modified DataFrame back to the CSV file
tornado_df.to_csv('./2000-2022_actual_tornadoes.csv')
