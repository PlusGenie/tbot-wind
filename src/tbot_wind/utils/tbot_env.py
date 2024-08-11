# -*- coding: utf-8 -*-
"TradingBoat © Copyright, Plusgenie Limited 2024. All Rights Reserved."
import os
import platform
from dotenv import load_dotenv

from .objects import EnvSettings

# Check if the .env file exists in the current working directory
current_dir_env_path = os.path.join(os.getcwd(), ".env")
home_dir_env_path = os.path.expanduser("~/.env")

# Fallback path based on the platform
if platform.system() == "Windows":
    fallback_env_path = os.path.join(os.getenv("APPDATA"), "tbot", ".env")
else:
    fallback_env_path = "/home/tbot/.env"

# Determine which .env file to use
if os.path.isfile(current_dir_env_path):
    ENV_FILE_PATH = current_dir_env_path
    print(f"Using .env file from current directory: {ENV_FILE_PATH}")
elif os.path.isfile(home_dir_env_path):
    ENV_FILE_PATH = home_dir_env_path
    print(f"Using .env file from home directory: {ENV_FILE_PATH}")
elif os.path.isfile(fallback_env_path):
    ENV_FILE_PATH = fallback_env_path
    print(f"Using fallback .env file: {ENV_FILE_PATH}")
else:
    ENV_FILE_PATH = None
    print(
        "No .env file found in current directory, home directory, or fallback path."
    )

# Load the environment variables from the chosen .env file
load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)

# Create an instance of the EnvSettings class to store and manage the environment variables
shared = EnvSettings()
