cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate
source .env

now=$(date +"%m_%d_%Y_%H:%M")

.venv/bin/python3.9 -m src.db_mongo --pages 2  --yolo_penthouse &> "logs/log_inside_${now}_penthouse"
.venv/bin/python3.9 -m telegram.tele_bot --penthouse &> "logs/telegram/log_${now}_tele_penthouse"