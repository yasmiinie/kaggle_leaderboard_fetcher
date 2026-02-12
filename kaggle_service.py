import os
import logging
import pandas as pd
from typing import Dict, List, Tuple, Optional
from kaggle.api.kaggle_api_extended import KaggleApi
from abc import ABC, abstractmethod
import math
import time
from datetime import datetime
import threading
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class LeaderboardObserver(ABC):
    @abstractmethod
    def update(self, competition_name: str, leaderboard_data: pd.DataFrame, changes: Optional[pd.DataFrame] = None):
        pass

class LeaderboardAnalytics(LeaderboardObserver):
    def __init__(self):
        self.leaderboard_history = {}
        self.score_changes = {}
        
    def update(self, competition_name: str, leaderboard_data: pd.DataFrame, changes: Optional[pd.DataFrame] = None):
        timestamp = datetime.now()
        
        self.leaderboard_history[competition_name] = {
            'timestamp': timestamp,
            'data': leaderboard_data.copy()
        }
        
        if changes is not None and not changes.empty:
            self.score_changes[competition_name] = {
                'timestamp': timestamp,
                'changes': changes.copy()
            }
            
            for _, change in changes.iterrows():
                if change['change_type'] == 'new_entry':
                    logging.info(f"New entry in {competition_name}: {change['teamName']} at position {change['new_position']}")
                else:
                    logging.info(
                        f"Position change in {competition_name}: {change['teamName']} "
                        f"moved from {change['old_position']} to {change['new_position']} "
                        f"(Score: {change['old_score']} -> {change['new_score']})"
                    )

class KaggleService:
    def __init__(self, refresh_interval: int = 300):
        # Check for kaggle.json
        kaggle_dir = os.path.expanduser('~/.kaggle')
        kaggle_json = os.path.join(kaggle_dir, 'kaggle.json')
        
        if not os.path.exists(kaggle_json):
            if not os.path.exists(kaggle_dir):
                os.makedirs(kaggle_dir)
            raise FileNotFoundError(
                "Kaggle API credentials not found. Please download kaggle.json from "
                "https://www.kaggle.com/account and place it in ~/.kaggle/"
            )

        # Ensure correct permissions
        os.chmod(kaggle_json, 0o600)

        try:
            self.api = KaggleApi()
            self.api.config_path = os.path.expanduser("~/.kaggle/kaggle.json")  # Assure-toi que le fichier existe
            self.api.authenticate()
            logging.info("Successfully authenticated with Kaggle API")
        except Exception as e:
            logging.error(f"Failed to authenticate with Kaggle API: {e}")
            raise

        self._observers: List[LeaderboardObserver] = []
        self._cached_leaderboards: Dict[str, pd.DataFrame] = {}
        self._competitions: Dict[str, float] = {}
        self.refresh_interval = refresh_interval
        self._monitoring = False
        self._monitor_thread = None

        # Define the list of CSV challenges
        self.csv_challenges = ["csv-challenge-1", "csv-challenge-2", "csv-challenge-3", "csv-challenge-4", "csv-challenge-5"]

    def start_monitoring(self, competitions: Dict[str, float]):
        """Start monitoring the specified competitions"""
        self._competitions = competitions
        if not self._monitoring:
            self._monitoring = True
            self._monitor_thread = threading.Thread(target=self._monitor_leaderboards)
            self._monitor_thread.daemon = True
            self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop monitoring competitions"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join()

    def _monitor_leaderboards(self):
        """Continuously monitor leaderboards for changes"""
        while self._monitoring:
            for competition_name in self._competitions:
                try:
                    self.fetch_leaderboard_data(competition_name)
                except Exception as e:
                    logging.error(f"Error monitoring competition {competition_name}: {e}")
            time.sleep(self.refresh_interval)

    def calculate_position_points(self, position: int) -> float:
        """Calculate points based on position with base points of 36"""
        return float(36 * math.exp(-0.2 * position))

    def fetch_leaderboard_data(self, competition_name: str) -> pd.DataFrame:
        """Fetch and process competition leaderboard data"""
        try:
            if competition_name in self.csv_challenges:
                try:
                    df = pd.read_csv(f"{competition_name}.csv")
                    if 'teamId' not in df.columns:
                        df['teamId'] = df['teamName'].apply(lambda x: hash(x))
                    df['score'] = df.index.to_series().apply(self.calculate_position_points)
                    return df
                except Exception as e:
                    logging.error(f"Error reading CSV file for {competition_name}: {e}")
                    return pd.DataFrame()

            try:
                leaderboard = self.api.competition_view_leaderboard(competition_name)
                
                if hasattr(leaderboard, 'submissions'):
                    data = leaderboard.submissions
                elif isinstance(leaderboard, dict) and 'submissions' in leaderboard:
                    data = leaderboard['submissions']
                else:
                    data = []
                    
                if not data:
                    logging.warning(f"No leaderboard data found for {competition_name}")
                    return pd.DataFrame()

                df = pd.DataFrame(data)
                
                if 'teamId' not in df.columns and 'teamName' in df.columns:
                    df['teamId'] = df['teamName'].apply(lambda x: hash(x))
                
                df['score'] = df.index.to_series().apply(self.calculate_position_points)

                changes = None
                if competition_name in self._cached_leaderboards:
                    old_df = self._cached_leaderboards[competition_name]
                    changes = self._detect_changes(old_df, df)
                    
                    if not changes.empty:
                        self._cached_leaderboards[competition_name] = df.copy()
                        self.notify_observers(competition_name, df, changes)
                        logging.info(f"Changes detected in {competition_name} leaderboard")
                else:
                    self._cached_leaderboards[competition_name] = df.copy()
                    self.notify_observers(competition_name, df)
                
                return df

            except Exception as e:
                logging.error(f"Error fetching leaderboard for {competition_name}: {e}")
                return pd.DataFrame()

        except Exception as e:
            logging.error(f"Error processing leaderboard for {competition_name}: {e}")
            return pd.DataFrame()

    def _detect_changes(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """Detect changes between old and new leaderboard data"""
        changes = []
        
        for index, row in new_df.iterrows():
            team_id = row['teamId']
            old_team_data = old_df[old_df['teamId'] == team_id]
            
            if old_team_data.empty:
                changes.append({
                    'teamId': team_id,
                    'teamName': row['teamName'],
                    'change_type': 'new_entry',
                    'old_position': None,
                    'new_position': index,
                    'old_score': None,
                    'new_score': row['score']
                })
            else:
                old_position = old_team_data.index[0]
                if old_position != index or old_team_data.iloc[0]['score'] != row['score']:
                    changes.append({
                        'teamId': team_id,
                        'teamName': row['teamName'],
                        'change_type': 'position_change',
                        'old_position': old_position,
                        'new_position': index,
                        'old_score': old_team_data.iloc[0]['score'],
                        'new_score': row['score']
                    })
                    
        return pd.DataFrame(changes)

    def attach(self, observer: LeaderboardObserver):
        """Attach an observer to the service"""
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: LeaderboardObserver):
        """Detach an observer from the service"""
        self._observers.remove(observer)

    def notify_observers(self, competition_name: str, leaderboard_data: pd.DataFrame, changes: Optional[pd.DataFrame] = None):
        """Notify all observers of changes"""
        for observer in self._observers:
            observer.update(competition_name, leaderboard_data, changes)

    def calculate_final_score(self, competitions: Dict[str, float]) -> pd.DataFrame:
        """Calculate final scores across all competitions"""
        all_results = []
        
        for competition, weight in competitions.items():
            try:
                df = self.fetch_leaderboard_data(competition)
                if not df.empty and 'teamName' in df.columns:
                    weighted_df = df.copy()
                    weighted_df['weighted_points'] = weighted_df['score'] * weight
                    result_df = weighted_df[['teamName', 'weighted_points']].copy()
                    result_df.loc[:, 'competition'] = competition
                    all_results.append(result_df)
                
            except Exception as e:
                logging.error(f"Error processing competition '{competition}': {e}")
                
        if not all_results:
            return pd.DataFrame()
            
        final_results = pd.concat(all_results, ignore_index=True)
        final_results = final_results.groupby(['teamName'], as_index=False)['weighted_points'].sum()
        final_results = final_results.sort_values(by='weighted_points', ascending=False).reset_index(drop=True)
        final_results['rank'] = final_results.index + 1
        
        return final_results