import os
import logging
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

class KaggleService:
    def __init__(self):
        self.api = KaggleApi()
        self.api.authenticate()

    def fetch_leaderboard_data(self, competition_name):
        # Fetch competition leaderboard data
        leaderboard = self.api.competition_view_leaderboard(competition_name)
        if hasattr(leaderboard, 'entries'):
            data = leaderboard.entries
        else:
            data = leaderboard.get('submissions', [])
        
        df = pd.DataFrame(data)
        logging.info(f"Fetched leaderboard for {competition_name}: {df.shape[0]} entries")
        return df

    def combine_leaderboards(self, competitions):
        leaderboards = []
        for competition, weight in competitions.items():
            try:
                df = self.fetch_leaderboard_data(competition)
                df['score'] = pd.to_numeric(df['score'], errors='coerce')
                df.dropna(subset=['score'], inplace=True)
                df['weighted_score'] = df['score'] * weight
                leaderboards.append(df)
            except Exception as e:
                logging.error(f"Error processing competition '{competition}': {e}")
        return pd.concat(leaderboards, ignore_index=True)
