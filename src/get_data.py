import pandas as pd
import os
import time

def load_data(raw_path, big5_leagues_only=False):
    # Load data into dict
    data_dict = {}

    for file in os.listdir(raw_path):

        if big5_leagues_only == True:
            if not any(league in file for league in ["premier-league", "1-bungesliga", "primera-divison",
                "ligue-1", "serie-a"]):
                continue

        filename = file.split(".")[0]
        filepath = os.path.join(raw_path, file)
        
        data_dict[filename] = pd.read_csv(filepath)

    return data_dict

def get_club_leagues(data_dict):
    # Create club and league pairing dict
    league_id_dict = {}
    league_id_dict["league_map"] = {}
    league_id_dict["country_map"] = {}

    for league, df in data_dict.items():
        clubs = df.club_name.unique().tolist()

        league_name = df["league_name"].unique()[0]
        country = df["country"].unique()[0]
        

        for club in clubs:
            league_id_dict["league_map"][club] = league_name
            league_id_dict["country_map"][club] = country

    return league_id_dict

def define_club_to(data_dict, league_id_dict, internal_transfers_only=True):
    # Map transfer destination to league
    if internal_transfers_only==True:
        for league, df in data_dict.items():
            df.rename(columns={'league_name':'league_from_name', 'country':'country_from_name'}, inplace=True)

            df["league_to_name"] = df["club_involved_name"].map(league_id_dict["league_map"])
            df["country_to_name"] = df["club_involved_name"].map(league_id_dict["country_map"])

    return data_dict

def remove_internal_duplicates(data_dict):
    # Drop duplicate transfers within the same league (SLOW)
    start=time.time()
    for league, df in data_dict.items():
        print(".loc ", league)
        drop_list_loc = []
        for index, row in df.iterrows():
            club = row["club_name"]
            player = row["player_name"]
            fee = row["fee_cleaned"]
            window = row["season"]
            period = row["transfer_period"]

            drop_index = df.loc[(df["club_involved_name"]==club) & 
                                (df["player_name"]==player) & 
                                (df["fee_cleaned"]==fee) & 
                                (df["season"]==window) &
                                (df["transfer_period"]==period)].index
            
            drop_list_loc.append(drop_index.tolist())
        
        df = df.drop(drop_list_loc)
    print("--- %s seconds for .loc ---" % (time.time() - start))
    print("end")
        # df = df.drop(drop_list_at)

def remove_external_duplicates(data_dict):
    # Remove transfers from 1 direction (as will be duplicated in other leagues)
    for league, df in data_dict.items():
        df = df[df.transfer_movement != "in"]

def export_data(data_dict, data_path, internal_transfers_only=True):
    # Export results
        for league, df in data_dict.items():
            if internal_transfers_only==True:
                # Create internal transfers df, containing only transfers between these leagues
                export_df = df.loc[df["league_to_name"].str.len() > 0]

                export_path = os.path.join(data_path, "processed", "internal-transfers")
                export_name = os.path.join(export_path, league+".csv")

                export_df.to_csv(export_name)

def main(big5_leagues_only=False, internal_transfers_only=True, remove_internal_dupes=False, remove_external_dupes=True):
    # Initialise directories
    cwd = os.path.dirname(__file__)
    data_path = os.path.abspath(os.path.join(cwd, "..", "data"))
    raw_path = os.path.join(data_path, "raw", "transfers")

    # Import data, munge and export
    data_dict = load_data(raw_path, big5_leagues_only)
    league_id_dict = get_club_leagues(data_dict)
    data_dict = define_club_to(data_dict, league_id_dict, internal_transfers_only)
    if remove_external_dupes==True:
        data_dict = remove_external_duplicates(data_dict)
    if remove_internal_dupes==True:
        data_dict = remove_internal_duplicates(data_dict)
    export_data(data_dict, data_path)

if __name__ == "__main__":
    main()