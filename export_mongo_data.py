#!/usr/bin/env python3
"""
Export all data from MongoDB to local JSON files.
Creates timestamped backup files in the data/ directory.
"""

import json
import os
from datetime import datetime
from pymongo import MongoClient
from bson import json_util
import argparse


def get_db(permission: str = "read"):
    """Connect to MongoDB using stored credentials."""
    with open(".db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)[permission]
    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = MongoClient(
        f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority"
    )
    mydb = cluster["flats"]
    return mydb


def export_collection(db, collection_name: str, output_path: str) -> int:
    """
    Export a single collection to a JSON file.
    Returns the number of documents exported.
    """
    collection = db[collection_name]
    documents = list(collection.find())
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(documents, f, default=json_util.default, indent=2, ensure_ascii=False)
    
    return len(documents)


def export_all(db, timestamp: str = None, combined: bool = False):
    """
    Export all collections from MongoDB.
    
    Args:
        db: MongoDB database connection
        timestamp: Custom timestamp for files (uses current time if None)
        combined: If True, save all data in a single combined file
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # List of collections to export
    collections = [
        "_flats",
        "photos",
        "_prices",
        "_prices_no_change",
        "_rent_prices",
        "_rent_prices_no_change",
    ]
    
    backup_dir = "data/backups"
    os.makedirs(backup_dir, exist_ok=True)
    
    print(f"\n📦 Starting MongoDB export (timestamp: {timestamp})\n")
    
    all_data = {}
    total_docs = 0
    
    for collection_name in collections:
        try:
            if combined:
                # Collect in memory for combined export
                collection = db[collection_name]
                docs = list(collection.find())
                all_data[collection_name] = docs
                total_docs += len(docs)
                print(f"✓ {collection_name:30s} {len(docs):8d} documents")
            else:
                # Export each collection separately
                output_path = f"{backup_dir}/{collection_name}_{timestamp}.json"
                count = export_collection(db, collection_name, output_path)
                total_docs += count
                print(f"✓ {collection_name:30s} {count:8d} documents → {output_path}")
        except Exception as e:
            print(f"✗ {collection_name:30s} ERROR: {str(e)}")
    
    if combined:
        # Save combined file
        combined_path = f"{backup_dir}/all_data_{timestamp}.json"
        with open(combined_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, default=json_util.default, indent=2, ensure_ascii=False)
        print(f"\n✓ Combined export: {combined_path}")
    
    print(f"\n✅ Export complete! Total documents: {total_docs}\n")
    
    return total_docs


def main():
    parser = argparse.ArgumentParser(
        description="Export MongoDB data to local JSON files"
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help="Export all collections into a single combined JSON file",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help="Custom timestamp for export files (format: YYYYMMDD_HHMMSS)",
    )
    
    args = parser.parse_args()
    
    try:
        print("🔗 Connecting to MongoDB...")
        db = get_db("read")
        # Test connection
        db.command("ping")
        print("✓ Connected successfully!")
        
        export_all(db, timestamp=args.timestamp, combined=args.combined)
        
    except FileNotFoundError:
        print("❌ ERROR: Could not find .db_creds/creds_mongo_mab.json")
        print("Make sure your credentials file exists and is in the correct location.")
        exit(1)
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
