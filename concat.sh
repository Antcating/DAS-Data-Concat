# if /media/NAS is not mounted, then mount it
if ! mountpoint -q /media/NAS; then
    mount -t cifs -o username=xxx,password=xxx,file_mode=0777,dir_mode=0777 //x.x.x.x/path /media/NAS/
fi
# Changing directory to main project directory
pushd PROJECT_PATH
# Activate venv FOR BASH (if you are using FISH, then use activate.fish)
source .venv_concat/bin/activate
# Running project
python3 src/concat.py
