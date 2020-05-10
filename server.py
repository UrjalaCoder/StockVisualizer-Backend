from flask import Flask

# Create the app instance
app = Flask(__name__)


@app.route("/")
def main():
    return "Works!"

