    
import os
import json
import logging
from pathlib import Path
import kaggle
import pandas as pd
from time import sleep
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kaggle_leaderboard.log'),
        logging.StreamHandler()
    ]
)

class KaggleLeaderboardService:
    def __init__(self):
        load_dotenv()
        self.setup_credentials()
        
    def setup_credentials(self):
        kaggle_json = {
            "username": os.getenv("KAGGLE_USERNAME"),
            "key":os.getenv("KAGGLE_KEY")
        }
        kaggle_dir = Path.home() / '.kaggle'
        kaggle_dir.mkdir(exist_ok=True)
        with open(kaggle_dir / 'kaggle.json', 'w') as f:
            json.dump(kaggle_json, f)
        os.chmod(kaggle_dir / 'kaggle.json', 0o600)
    

        
    def fetch_leaderboard(self, competition_name, refresh_interval=30):
        while True:
            try:
                kaggle.api.authenticate()
                leaderboard = kaggle.api.competition_view_leaderboard(competition_name)
                
                # Convert leaderboard object to DataFrame
                if hasattr(leaderboard, 'entries'):
                    data = leaderboard.entries
                else:
                    # Handle new API format
                    data = leaderboard['submissions'] if isinstance(leaderboard, dict) else leaderboard
                
                df = pd.DataFrame(data)
                
                # Flexible column selection
                columns = ['teamId', 'teamName', 'submissionDate', 'score']
                available_columns = [col for col in columns if col in df.columns]
                df = df[available_columns]
                
                if 'submissionDate' in df.columns:
                    df['submissionDate'] = pd.to_datetime(df['submissionDate'])
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'leaderboard_{timestamp}.csv'
               # df.to_csv(filename, index=False)
                
                logging.info(f"Leaderboard updated: {filename}")
                logging.info(f"Current standings:\n{df.head().to_string()}")
                
                sleep(refresh_interval)
                
            except Exception as e:
                logging.error(f"Error fetching leaderboard: {e}")
                sleep(refresh_interval)

if __name__ == "__main__":
    service = KaggleLeaderboardService()
    service.fetch_leaderboard(os.getenv("KAGGLE_SLUG"))