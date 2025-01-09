from flask import Flask, jsonify, request
from kaggle_service import KaggleService # import kaggle apis li khdmthom doka
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

kaggle_service = KaggleService()

@app.route('/leaderboard', methods=['GET'])
def get_leaderboard():
    competitions = {
        # competion slug : it's weight 
        "1b22f135b4d3a31152fa": 1.0, 
        "DATAHACK-store-recommendation-system": 1.0,
        "DATAHACK-Yalidine-Challenge": 1.0,
    }
    final_ranking = kaggle_service.calculate_final_score(competitions)
    
   # print(final_ranking[['overall_rank', 'teamName', 'total_points', 'competition_performances']])
    return jsonify(final_ranking.to_dict(orient='records'))

@app.route('/challenge', methods=['GET']) 
def get_leaderboardd():
    return jsonify(kaggle_service.fetch_leaderboard_data("1b22f135b4d3a31152fa").to_dict(orient='records'))

if __name__ == "__main__":
    app.run(debug=True)
