import os
import logging
import pandas as pd
from typing import Dict, List, Tuple
from kaggle.api.kaggle_api_extended import KaggleApi
from abc import ABC, abstractmethod

# Observer Interface
class LeaderboardObserver(ABC):
    @abstractmethod
    def update(self, competition_name: str, leaderboard_data: pd.DataFrame):
        pass

# Subject (Observable)
class KaggleService:
    def __init__(self):
        self.api = KaggleApi()
        self.api.authenticate()
        self._observers: List[LeaderboardObserver] = []
        self._cached_leaderboards: Dict[str, pd.DataFrame] = {}

    def attach(self, observer: LeaderboardObserver):
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: LeaderboardObserver):
        self._observers.remove(observer)

    def notify_observers(self, competition_name: str, leaderboard_data: pd.DataFrame):
        for observer in self._observers:
            observer.update(competition_name, leaderboard_data)
            
    def calculate_position_points(self, position: int, base_points: int = 30) -> float:
        """Calculate points based on position with base points of 30"""
        return  base_points - (position)

    def fetch_leaderboard_data(self, competition_name: str) -> pd.DataFrame:
        # Fetch competition leaderboard data
        leaderboard = self.api.competition_view_leaderboard(competition_name)
        
        if hasattr(leaderboard, 'entries'):
            data = leaderboard.entries
        else:
            data = leaderboard.get('submissions', [])
            
        df = pd.DataFrame(data)
        df['score'] = df.index.to_series().apply(self.calculate_position_points)

        
        # Check if the leaderboard has changed
        if (competition_name not in self._cached_leaderboards or 
            not df.equals(self._cached_leaderboards[competition_name])):
            self._cached_leaderboards[competition_name] = df.copy()
            self.notify_observers(competition_name, df)
            
        logging.info(f"Fetched leaderboard for {competition_name}: {df.shape[0]} entries")
        return df
    
    def calculate_final_score(self, competitions: Dict[str, float]) -> pd.DataFrame:
        all_results = []
        competition_leaderboards = {}

        for competition, weight in competitions.items():
            try:
                df = self.fetch_leaderboard_data(competition)
                df['score'] = df.index.to_series().apply(self.calculate_position_points)
                df['score'] = pd.to_numeric(df['score'], errors='coerce')

                # Apply competition weight
                df['weighted_points'] = df['score'] * weight

                # Sum the weighted points for each team
                total_weighted_points = df.groupby('teamId')['weighted_points'].sum().reset_index()
                total_weighted_points = total_weighted_points.rename(columns={'weighted_points': 'total_weighted_points'})

                # Merge the total weighted points back to the original dataframe
                df = df.merge(total_weighted_points, on='teamId', how='left')

                # Order by total weighted points
                df = df.sort_values(by='total_weighted_points', ascending=False)

                competition_leaderboards[competition] = df[
                    ['teamId', 'teamName', 'total_weighted_points']
                ].copy()

                # Prepare results for the detailed view
                result_df = df[['teamId', 'teamName', 'total_weighted_points']]
                result_df.loc[:, 'competition'] = competition

                all_results.append(result_df)

            except Exception as e:
                logging.error(f"Error processing competition '{competition}': {e}")

        # Combine all results into a single DataFrame
        final_results = pd.concat(all_results, ignore_index=True)
        # Verify if the teamId is the same and add the total_weighted_points
        final_results = final_results.groupby(['teamName'], as_index=False)['total_weighted_points'].sum()
        
        # Sort by total_weighted_points
        final_results = final_results.sort_values(by='total_weighted_points', ascending=False).reset_index(drop=True)
        return final_results



    def combine_leaderboards(self, competitions: Dict[str, float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Combine leaderboards with position-based scoring and create final overall ranking
        
        Args:
            competitions: Dict[str, float] where key is competition name and value is weight
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: First DataFrame contains detailed results,
            second DataFrame contains final overall ranking
        """
        all_results = []
        competition_leaderboards = {}
        
        # Process each competition
        for competition, weight in competitions.items():
            try:
                df = self.fetch_leaderboard_data(competition)
                df['score'] = pd.to_numeric(df['score'], errors='coerce')

                
                # Apply competition weight
                df['weighted_points'] = df['score'] * weight
                
                # Store competition-specific leaderboard
                competition_leaderboards[competition] = df[
                    ['teamId', 'teamName', 'weighted_points']
                ].copy()
                
                # Prepare results for the detailed view
                result_df = df[['teamId', 'teamName', 'weighted_points']]
                result_df['competition'] = competition
                
                all_results.append(result_df)
                
            except Exception as e:
                logging.error(f"Error processing competition '{competition}': {e}")
                
        if not all_results:
            return pd.DataFrame(), pd.DataFrame()
            
        # Combine all detailed results
        detailed_results = pd.concat(all_results, ignore_index=True)
        
        # Calculate total points per team for the detailed view
        detailed_summary = (detailed_results.groupby(['teamId', 'teamName'])
                          .agg({
                              'weighted_points': 'sum',
                              'competition': lambda x: ', '.join(x),
                          })
                          .reset_index()
                          .rename(columns={
                              'weighted_points': 'total_points',
                              'competition': 'competitions',
                             
                          }))
        
        # Create final overall ranking
        final_ranking = []
        for teamId in detailed_summary['teamId'].unique():
            team_data = {
                'teamId': teamId,
                'teamName': detailed_summary[detailed_summary['teamId'] == teamId]['teamName'].iloc[0],
                'total_points': detailed_summary[detailed_summary['teamId'] == teamId]['total_points'].iloc[0],
                'competition_performances': {}
            }
            
            # Add performance details for each competition
            for competition in competitions:
                if competition in competition_leaderboards:
                    comp_data = competition_leaderboards[competition]
                    team_comp_data = comp_data[comp_data['teamId'] == teamId]
                    if not team_comp_data.empty:
                        team_data['competition_performances'][competition] = {
                            'points': float(team_comp_data['weighted_points'].iloc[0])
                        }
                    else:
                        team_data['competition_performances'][competition] = {
                            'points': 0.0
                        }
            
            final_ranking.append(team_data)
        
        # Convert final ranking to DataFrame and sort by total points
        final_ranking_df = pd.DataFrame(final_ranking)
        final_ranking_df = final_ranking_df.sort_values('total_points', ascending=False).reset_index(drop=True)
        final_ranking_df['overall_rank'] = final_ranking_df.index + 1
        
        return detailed_summary, final_ranking_df

# Example Observer Implementation
class LeaderboardAnalytics(LeaderboardObserver):
    def __init__(self):
        self.leaderboard_history = {}
        
    def update(self, competition_name: str, leaderboard_data: pd.DataFrame):
        self.leaderboard_history[competition_name] = {
            'timestamp': pd.Timestamp.now(),
            'data': leaderboard_data.copy()
        }
        logging.info(f"Leaderboard updated for {competition_name} at {pd.Timestamp.now()}")