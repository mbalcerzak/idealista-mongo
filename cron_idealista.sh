cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

now=$(date +"%m_%d_%Y")
.venv/bin/python3.9 -m src.db_mongo --pages 20 &> "logs/log_inside_${now}"
.venv/bin/python3.9 -m src.db_mongo --pages 3  --yolo_penthouse &> "logs/log_inside_${now}_penthouse"
.venv/bin/python3.9 -m src.db_mongo --pages 4  --rent &> "logs/log_inside_${now}_rent"
.venv/bin/python3.9 -m src.db_mongo --pages 4  --rent_penthouse &> "logs/log_inside_${now}_rent_penthouse"

.venv/bin/python3.9 -m src.flat_stats &> "logs/log_inside_${now}_price_stats"
.venv/bin/python3.9 -m telegram.tele_bot &> "logs/log_inside_${now}_telegram"