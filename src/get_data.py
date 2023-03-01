import pandas as pd
import os

def load_data(raw_path, big_leagues_only=True):
    # Load data into dict
    data_dict = {}

    for file in os.listdir(raw_path):

        if big_leagues_only == True:
            if any(ignore in file for ignore in ["premier-liga", "championship"]):
                continue

        filename = file.split(".")[0]
        filepath = os.path.join(raw_path, file)
        
        data_dict[filename] = pd.read_csv(filepath)

    return data_dict

def get_club_leagues(data_dict):
    # Create club and league pairing dict
    league_id_dict = {}

    for league, df in data_dict.items():
        clubs = df.club_name.unique().tolist()

        for club in clubs:
            league_id_dict[club] = league

    return league_id_dict

def define_club_to(data_dict, league_id_dict, internal_transfers_only=True):
    # Map transfer destination to league
    if internal_transfers_only==True:
        for league, df in data_dict.items():
            df.rename(columns={'league_name':'league_from_name'}, inplace=True)

            df["league_to_name"] = df["club_involved_name"].map(league_id_dict)

    return data_dict

def export_data(data_dict, data_path, internal_transfers_only=True):
    # Export results
        for league, df in data_dict.items():
            if internal_transfers_only==True:
                # Create internal transfers df, containing only transfers between these leagues
                export_df = df.loc[df["league_to_name"].str.len() > 0]

                export_path = os.path.join(data_path, "processed", "internal_transfers")
                export_name = os.path.join(export_path, league)

                export_df.to_csv(export_name)

def main(big_leagues_only=True, internal_transfers_only=True):
    cwd = os.path.dirname(__file__)
    data_path = os.path.abspath(os.path.join(cwd, "..", "data"))
    raw_path = os.path.join(data_path, "raw", "transfers")

    data_dict = load_data(raw_path, big_leagues_only)
    league_id_dict = get_club_leagues(data_dict)
    data_dict = define_club_to(data_dict, league_id_dict, internal_transfers_only)
    export_data(data_dict, data_path)

if __name__ == "__main__":
    main()