from flask import Flask, jsonify, request
from kaggle_service import KaggleService # import kaggle apis li khdmthom doka
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

kaggle_service = KaggleService()

@app.route('/leaderboard/', methods=['GET'])
def get_leaderboard():
    competitions = {
        # competion slug : it's weight 
        "8b971ca63765": 1.0, 
        "DATAHACK-Yalidine-Challenge": 1.0,
        #"competition3": 1.2,
    }
    leaderboard = kaggle_service.combine_leaderboards(competitions)
    return jsonify(leaderboard.to_dict(orient='records'))

if __name__ == "__main__":
    app.run(debug=True)
