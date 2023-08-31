from flask import Flask
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/flaskCrashCourse"
mongo = PyMongo(app)


@app.route('/')
def home():
    return "<p>The home page</p>"

if __name__ == "__main__":
    app.run(debug=True)