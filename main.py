# from src.gmaps import Gmaps
# import time
# import pandas as pd
# from casefy import kebabcase

# coordinates = pd.read_excel("C:/Users/ADMIN/google-map/src/xlsx/coordinates-quan1-hcm_v3.xlsx")
# geo_coordinates  = [i for i in coordinates["lat_long"]]
# start_time = time.time()
# fields = [
#    Gmaps.Fields.PLACE_ID, 
#    Gmaps.Fields.NAME, 
#    Gmaps.Fields.MAIN_CATEGORY, 
#    Gmaps.Fields.PHONE, 
#    Gmaps.Fields.FACEBOOK,
#    Gmaps.Fields.WEBSITE,
#    Gmaps.Fields.ADDRESS,
#    Gmaps.Fields.DETAILED_ADDRESS,
#    Gmaps.Fields.COORDINATES,
# ]


# queries = ["homestay"
#            ]
# max = 30

# Gmaps.places(queries, geo_coordinates = geo_coordinates, zoom = 19 , max = max, fields=fields, convert_to_english = False ,lang=Gmaps.Lang.Vietnamese)

# end_time = time.time()
# elapsed_time = end_time - start_time

# print(f"Thời gian chạy: {elapsed_time/60} phút")

# xl = kebabcase(queries[0]) + "-" + str(max)
# df = pd.read_csv(f"C:/Users/ADMIN/google-map/output/quan1_hcm/{xl}/csv/places-of-{xl}.csv").drop_duplicates()
# print(f"Số dòng: {df.shape[0]}")
# df.to_excel(f"C:/Users/ADMIN/google-map/src/xlsx/quan1_hcm/{queries[0]}.xlsx")
import requests
import threading
import time
import pandas as pd

df_test = pd.read_excel("C:/Users/ADMIN/google-map/src/xlsx/quan1_hcm/total_quan1_hcm.xlsx")


def get_location(lat, long, api_key):
    #api_key = "mwA5L1GvWFvnwIYL-U1kP0RDsZ61ElOKAH7wnbsTs4M"
    url = f'https://revgeocode.search.hereapi.com/v1/revgeocode?apikey={api_key}&at={lat},{long}'
    
    try:
        # Gửi yêu cầu API
        response = requests.get(url)

        if response.status_code == 429:
            return { "House_number": "", "Street": "", "Ward": "", "District": "", "City": "", "progress": "429" }
        else: 
            # Xử lý kết quả và trả về
            data = response.json()
            address = data['items'][0]['address']
            city = address.get('county', '')
            district = address.get('city', '')
            ward = address.get('district', '')
            street = address.get('street', '')
            houseNumber = address.get('houseNumber', '')
            result = { "House_number": houseNumber, "Street": street, "Ward": ward, "District": district, "City": city, "progress": "True" }
    except:
        result = { "House_number": "", "Street": "", "Ward": "", "District": "", "City": "", "progress": "False" }
    return result

def out_put(df, api_key):
    house_numbers = []
    streets = []
    wards = []
    districts = []
    cities = []
    progresses = []
    
    for lat, long in zip(df["lat"], df["lng"]):
        address_info = get_location(lat, long, api_key)
        house_numbers.append(address_info["House_number"])
        streets.append(address_info["Street"])
        wards.append(address_info["Ward"])
        districts.append(address_info["District"])
        cities.append(address_info["City"])
        progresses.append(address_info["progress"])
    
    df["House_number"] = house_numbers
    df["Street"] = streets
    df["Ward"] = wards
    df["District"] = districts
    df["City"] = cities
    df["progress"] = progresses

    return df

# Calculate the number of groups
num_groups = len(df_test) // 1000

# Calculate the number of remaining rows
remaining_rows = len(df_test) % 1000

# Create a list to store the new dataframes
dfs = []

# Split the dataframe into groups of two rows each
for i in range(num_groups):
    start_index = i * 1000
    end_index = start_index + 1000
    df_group = df_test[start_index:end_index].copy()
    dfs.append(df_group)

# Add the remaining rows to the last dataframe
if remaining_rows > 0:
    last_df = df_test[-remaining_rows:].copy()
    dfs.append(last_df)


# tiến hành chạy multi thread

final = []
def out_put_with_time(df, api_key):
    start_time = time.time()
    out_put(df, api_key)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Time: {execution_time/60} phút")

for df , api_key in zip(dfs, list_api_keys):
    thread = threading.Thread(target=out_put_with_time, args=(df,api_key))
    thread.start()
    final.append(df)

print("Done")

merged_df = pd.concat(final, ignore_index=True)

merged_df.to_excel("C:/Users/ADMIN/google-map/src/xlsx/quan1_hcm/total_quan1_hcm_fix.xlsx")