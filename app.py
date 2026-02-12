import http.server
import math
import socketserver
import json
import logging
import threading
import time
import traceback
import os
import sys
from typing import Dict

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure Kaggle API credentials exist
kaggle_json_path = os.path.expanduser('~/.kaggle/kaggle.json')
if not os.path.exists(kaggle_json_path):
    logger.error("Kaggle API credentials not found. Please follow the setup instructions.")
    sys.exit(1)

# Import the Kaggle service
try:
    from kaggle_service import KaggleService, LeaderboardAnalytics
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    sys.exit(1)

# Initialize the Kaggle service
try:
    kaggle_service = KaggleService(refresh_interval=300)  # 5 minutes
    analytics = LeaderboardAnalytics()
    kaggle_service.attach(analytics)
    
    # Define competitions and their weights
    competitions = {
        "csv-challenge-1": 0.18,  
        "csv-challenge-2": 0.18,
        "csv-challenge-3": 0.16,
        "csv-challenge-4": 0.18,
        "csv-challenge-5": 0.30
    }
    
    # Start monitoring in a background thread
    def start_monitoring():
        try:
            kaggle_service.start_monitoring(competitions)
        except Exception as e:
            logger.error(f"Error in monitoring thread: {e}")
            logger.error(traceback.format_exc())
    
    monitoring_thread = threading.Thread(target=start_monitoring)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    
except Exception as e:
    logger.error(f"Failed to initialize Kaggle service: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class LeaderboardHandler(http.server.SimpleHTTPRequestHandler):
    def send_cors_headers(self):
        """Send CORS headers"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type')

    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS"""
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/api/leaderboard':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_cors_headers()
            self.end_headers()
            
            try:
                if kaggle_service is None:
                    self.wfile.write(json.dumps({
                        "error": "Kaggle service not initialized"
                    }).encode())
                    return
                
                final_scores = kaggle_service.calculate_final_score(competitions)
                
                if final_scores.empty:
                    self.wfile.write(json.dumps({"data": []}).encode())
                    return
                
                # Transform DataFrame to list of dicts for JSON response
                result = []
                for _, row in final_scores.iterrows():
                    team_scores = []
                    for competition, weight in competitions.items():
                        try:
                            if competition.startswith("csv-challenge"):
                                # ydi CSV challenges
                                csv_file = f"{competition}.csv"
                                if os.path.exists(csv_file):
                                    csv_data = pd.read_csv(csv_file)
                                    team_position = csv_data[csv_data['teamName'] == row['teamName']].index
                                    score = float(36 * math.exp(-0.2 * team_position[0]) * weight) if len(team_position) > 0 else 0.0
                                else:
                                    score = 0.0
                            else:
                                # ydi Kaggle competitions
                                comp_data = kaggle_service._cached_leaderboards.get(competition, None)
                                if comp_data is not None and not comp_data.empty:
                                    team_row = comp_data[comp_data['teamName'] == row['teamName']]
                                    score = float(team_row.iloc[0]['score'] * weight) if not team_row.empty else 0.0
                                else:
                                    score = 0.0
                            team_scores.append(score)
                        except Exception as e:
                            logger.error(f"Error calculating score for {row['teamName']} in {competition}: {e}")
                            team_scores.append(0.0)
                    
                    result.append({
                        "rank": int(row['rank']),
                        "team": row['teamName'],
                        "scores": team_scores,
                        "total": float(row['weighted_points'])
                    })
                
                response_data = {
                    "data": result,
                    "competitions": list(competitions.keys())
                }
                
                self.wfile.write(json.dumps(response_data, default=str).encode())
                
            except Exception as e:
                logger.error(e)       
        
if __name__ == "__main__":
    PORT = 8000
    
    try:
        with socketserver.TCPServer(("", PORT), LeaderboardHandler) as httpd:
            print(f"Serving at http://localhost:{PORT}")
            print(f"Test endpoint: http://localhost:{PORT}/test")
            print(f"Leaderboard endpoint: http://localhost:{PORT}/api/leaderboard")
            print("Press Ctrl+C to stop the server")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down the server...")
        if kaggle_service:
            kaggle_service.stop_monitoring()
        print("Server stopped")