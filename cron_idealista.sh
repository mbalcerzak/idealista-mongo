cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 src/db_mongo.py > "log_inside_${now}"

.venv/bin/python3.9 src/flat_info.py
