import os
from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import find_dotenv, load_dotenv
import json
from os import environ as env
import pandas as pd

ENV_FILE = find_dotenv()

if ENV_FILE:
    load_dotenv(ENV_FILE)

app = Flask(__name__)   
app.secret_key = env.get("APP_SECRET_KEY")

app.config['UPLOAD_FOLDER'] = 'uploads'
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

@app.route('/')
def home():
    return render_template("/index.html")

@app.route('/images/<img_src>')
def get_image(img_src):
    return send_from_directory('static/images', img_src)

@app.route('/templates/<html_src>')
def get_html(html_src):
    return send_from_directory('templates', html_src)

@app.route('/login', methods=["GET", "POST"])
def logged_in():
    username = request.form['username'].strip()
    password = request.form['password'].strip()

    print("Loaded Username: ", env.get("USER"))
    print("Loaded Password: ", env.get("PASSWORD"))
    print("Trimmed Username: '", username, "'", sep="")
    print("Trimmed Password: '", password, "'", sep="")

    if (username != env.get("USER")) or (password != env.get("PASSWORD")):
        print(username != env.get("USER"))
        print(password != env.get("PASSWORD"))
        return jsonify({'success': False, 'msg': 'Invalid Username and/or Password'})
    else:
        return jsonify({'success': True})

@app.route('/upload-data', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"message": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"message": "No selected file"}), 400

    # Save the file
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
    return jsonify({"message": "File successfully uploaded"}), 200

