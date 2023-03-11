import pandas as pd 
import os

# read data
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw", "transfers"))
data_dict = {}
for file in os.listdir(data_path):
    filepath = os.path.join(data_path, file)
    data_dict[file] = pd.read_csv(filepath)

# Get duplicates
data_duplicate = {}

for k, v in data_dict.items():
    print(f"calculating dupes for {k}...")
    
    dupes_df = pd.DataFrame()
    for index, row in v.iterrows():
        club = row["club_name"]
        player=row["player_name"]
        fee=row["fee_cleaned"]
        window = row["season"]

        df = v.loc[(v["club_involved_name"]==club) & (v["player_name"]==player) & (v["fee_cleaned"]==fee) & (v["season"]==window)]

        if len(df) > 0:
            df = pd.concat([df, pd.DataFrame(row).T])
            dupes_df = pd.concat([df, dupes_df])

    data_duplicate[k] = dupes_df

# save to csv
combined_df = pd.DataFrame()
for k, v in data_duplicate.items():
    combined_df = pd.concat([combined_df, v])

#combined_df.to_csv("duplicated_transfers.csv")
