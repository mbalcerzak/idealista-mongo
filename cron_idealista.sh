cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 src/db_mongo.py --pages 25 &> "logs/log_inside_${now}"
# .venv/bin/python3.9 src/db_mongo.py --pages 10  --mab &> "logs/log_inside_${now}_mab"
.venv/bin/python3.9 src/db_mongo.py --pages 10  --yolo_penthouse &> "logs/log_inside_${now}_penthouse"

.venv/bin/python3.9 src/flat_info.py
# .venv/bin/python3.9 src/penthouse.py
.venv/bin/python3.9 src/flat_stats.py