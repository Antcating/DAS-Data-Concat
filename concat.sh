# Changing directory to main project directory
pushd PROJECT_PATH
# Activate venv FOR BASH (if you are using FISH, then use activate.fish)
source .venv/bin/activate
# Running project
python3 src/concat.py
