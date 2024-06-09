cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 -m src.db_mongo --pages 2 &> "logs/log_inside_${now}"
.venv/bin/python3.9 -m src.db_mongo --pages 2  --yolo_penthouse &> "logs/log_inside_${now}_penthouse"
.venv/bin/python3.9 -m src.db_mongo --pages 2  --rent &> "logs/log_inside_${now}_rent"
.venv/bin/python3.9 -m src.db_mongo --pages 2  --rent_penthouse &> "logs/log_inside_${now}_rent_penthouse"

# # .venv/bin/python3.9 src/penthouse.py
.venv/bin/python3.9 -m src.flat_stats &> "logs/log_inside_${now}_price_stats"