cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 src/db_mongo.py --pages 20 &> "logs/log_inside_${now}"
.venv/bin/python3.9 src/db_mongo.py --pages 3  --yolo_penthouse &> "logs/log_inside_${now}_penthouse"
.venv/bin/python3.9 src/db_mongo.py --pages 4  --rent &> "logs/log_inside_${now}_rent"
.venv/bin/python3.9 src/db_mongo.py --pages 2  --rent_penthouse &> "logs/log_inside_${now}_rent_penthouse"

# .venv/bin/python3.9 src/penthouse.py
.venv/bin/python3.9 src/flat_stats.py &> "logs/log_inside_${now}_price_stats"