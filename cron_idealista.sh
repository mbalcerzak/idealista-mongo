cd /Users/malgorzatabslcerzak/Documents/projects/idealista-mongo/

source .venv/bin/activate

.venv/bin/python3.9 src/db_mongo.py &> log_inside

.venv/bin/python3.9 src/flat_info.py
