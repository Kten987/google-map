import requests
import threading
import time
import pandas as pd

df_test = pd.read_excel("C:/Users/ADMIN/google-map/src/xlsx/quan1_hcm/total_quan1_hcm.xlsx")

list_api_keys = ["1EotnhALO_Xf7d5mlOVefECdfpbgPG_Dik0TeOcQauQ",
"92hxCAU523jWUFRXMhM-p3w66P3PxYfY_tZm9pPeShU",
"KwvOafjkobVhF8Iz63lMyNooS-hjaYgmHdZrqVVVHWw",
"N5qb2q5dSfly3fnqWMsDPqVMWUd1yRbTBxbT34hvw_g",
"SsQNhtNfemZJr_FzCYZlub29VeTTjymT3Qm6syC50W8",
"br4m9W3KMhrPa51Vru5M1mWJYCfaVFvWiP3kB2gEQUA",
"spdVYGb-b6cgokZBOuwGG4b3WbmgsxQqv7tudjdOfLc",
"0zeR-GEwo5Od-poIZYTGrw-I80fC2C0b0q4F1Boa8oc",
"UfHD68dH4O9ld-NS22Se6Bls98P9aPZhFq4hA_H7-74",
"c6NrKs_BpAX4OdRFKAVSDeUaQTjSpNF8dCxjcLT6AfE",
"WCpDyLiAACoL9zwSFQQa_HboP-HdWXSM601W1N02aeg",
"faJrm5egL12NmLivrDsbqz44I47DhbJQUIg89L3TdIo",
"cdXQHZ3zzsZv-n04kjimcml7e3BRKcR2H3wl8XPi6SU",
"RBRLSkMgpwy6QF0jKsXs-jXEliqtAwwD_jd0DbHrEN8",
"Dd-n66bTKOIKeQ0H6nRvIOMWatnJ_viPxKMNGpxsFa4",
"q-Ao_RRfZNyfQPp2XsnYGbrOItF6SlXN0jObPIKVnk",
"ufLjrWIrvjyLHzL1TZIEOZjKpgolDMBUPXUwoIblgs0",
"tvjh1g0Xygp6DMsH8n5hX41jgbNh3VE61MsnWHYkfZw",
"d_8_FhktHjeMh91gB9bBKGHgMJFC6auZqHThhlHL9Hw",
"P1RAwTjkXvZUQLjHyi06Zm1YP1KeaZMJdjaNN1ZeOeE",
"DQV3KQ2U9DLoro0jMQUoIr-PQMKBpKIEpOqXzF1A498",
"6IcCytBh4PkKOC3YE2Smw4IGdAyTD7XTtsmgtxF-kdY",
"PgxoArILmRDFQreVp3pC6bexYfcKIVwMMN4oKpa8VOo",
"mwA5L1GvWFvnwIYL-U1kP0RDsZ61ElOKAH7wnbsTs4M"]

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
    thread.join()
    final.append(df)

print("Done")

merged_df = pd.concat(final, ignore_index=True)

