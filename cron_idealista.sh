cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 src/db_mongo.py --pages 20 &> "log_inside_${now}"
.venv/bin/python3.9 src/db_mongo.py --pages 10  --mab &> "log_inside_${now}_mab"

.venv/bin/python3.9 src/flat_info.py
