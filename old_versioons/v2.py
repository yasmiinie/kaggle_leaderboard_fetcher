from flask import Flask
from flask_socketio import SocketIO, emit
import threading
import time
from dotenv import load_dotenv
from kaggle_leaderboard_service import KaggleLeaderboardService  # Assuming this is your service class

# Initialize Flask and SocketIO
app = Flask(__name__)
socketio = SocketIO(app)

# Load environment variables
load_dotenv()

# Initialize the KaggleLeaderboardService
service = KaggleLeaderboardService()

# Define the leaderboard data variable
leaderboard_data = {}

def background_fetch_leaderboard():
    global leaderboard_data
    while True:
        try:
            # Fetch leaderboard data
            leaderboard_data = service.fetch_leaderboard(os.getenv("KAGGLE_SLUG"))
            # Emit the leaderboard data to all connected clients
            socketio.emit('update_leaderboard', leaderboard_data)
            time.sleep(30)
        except Exception as e:
            print(f"Error fetching leaderboard: {e}")
            time.sleep(30)

@socketio.on('connect')
def on_connect():
    emit('update_leaderboard', leaderboard_data)  # Send initial data to the client on connect

if __name__ == "__main__":
    # Start the background thread for fetching leaderboard data
    threading.Thread(target=background_fetch_leaderboard, daemon=True).start()
    # Run the Flask-SocketIO app
    socketio.run(app, debug=True, port=5000)
