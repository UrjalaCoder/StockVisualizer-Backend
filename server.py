from flask import Flask, render_template, url_for

# Create the app instance
app = Flask(__name__)

# Basic index routing
@app.route("/index")
@app.route("/")
def main():
    return render_template("index.html")

