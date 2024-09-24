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

@app.route('/')
def home():
    return render_template("/index.html")

