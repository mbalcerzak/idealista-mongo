import pymongo
from pymongo import errors
import json
import logging
from argparse import ArgumentParser
from typing import Dict, List, Optional, Any
from PIL import Image
import requests
from io import BytesIO

from src.crawler_api import get_flats, SearchType

# Configuration constants
DB_CREDENTIALS_FILE = ".db_creds/creds_mongo_mab.json"
MONGODB_CONNECTION_STRING = "mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "flats"

COLLECTION_NAMES = {
    "flats": "_flats",
    "photos": "photos",
    "prices_sale": "_prices",
    "prices_sale_no_change": "_prices_no_change",
    "prices_rent": "_rent_prices",
    "prices_rent_no_change": "_rent_prices_no_change",
}

OUTPUT_FILES = {
    "newest_flats": "output/newest_flats.json",
    "newest_penthouses": "output/newest_penthouses.json",
}

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Argument to SearchType mapping
SEARCH_TYPE_MAP = {
    "mab": SearchType.MAB,
    "house": SearchType.HOUSE,
    "yolo_penthouse": SearchType.YOLO_PENTHOUSE,
    "rent": SearchType.RENT,
    "rent_penthouse": SearchType.RENT_PENTHOUSE,
}


def get_db(permission: str = "read") -> pymongo.database.Database:
    """
    Connect to MongoDB database using stored credentials.
    
    Args:
        permission: Credential type to use ('read' or 'admin')
        
    Returns:
        MongoDB database connection
    """
    with open(DB_CREDENTIALS_FILE, "r") as f:
        creds_mongo = json.load(f)[permission]
    
    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(
        MONGODB_CONNECTION_STRING.format(username=username, password=password)
    )
    return cluster[DB_NAME]


def get_image_idealista(image_url: str) -> Optional[bytes]:
    """
    Download and process image from Idealista URL.
    
    Args:
        image_url: URL of the image to download
        
    Returns:
        Image bytes or None if download/processing fails
    """
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        image = Image.open(BytesIO(response.content))
        image_bytes = BytesIO()
        image.save(image_bytes, format=image.format)
        
        return image_bytes.getvalue()
    except (requests.RequestException, IOError) as e:
        logger.warning(f"Failed to download image from {image_url}: {e}")
        return None


def args_to_search_type(args) -> SearchType:
    """
    Convert argument flags to SearchType enum.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Corresponding SearchType
    """
    for flag, search_type in SEARCH_TYPE_MAP.items():
        if getattr(args, flag, False):
            return search_type
    return SearchType.STANDARD


def get_collections(db: pymongo.database.Database, is_rent: bool) -> Dict[str, pymongo.collection.Collection]:
    """
    Get the appropriate collections based on operation type.
    
    Args:
        db: MongoDB database connection
        is_rent: Whether searching for rental properties
        
    Returns:
        Dictionary with collection references
    """
    if is_rent:
        logger.info("📌 Processing rental properties")
        return {
            "flats": db[COLLECTION_NAMES["flats"]],
            "photos": db[COLLECTION_NAMES["photos"]],
            "prices": db[COLLECTION_NAMES["prices_rent"]],
            "prices_no_change": db[COLLECTION_NAMES["prices_rent_no_change"]],
        }
    else:
        logger.info("📌 Processing properties for sale")
        return {
            "flats": db[COLLECTION_NAMES["flats"]],
            "photos": db[COLLECTION_NAMES["photos"]],
            "prices": db[COLLECTION_NAMES["prices_sale"]],
            "prices_no_change": db[COLLECTION_NAMES["prices_sale_no_change"]],
        }


def add_flat_to_db(
    flat: Dict[str, Any], 
    collections: Dict[str, pymongo.collection.Collection]
) -> bool:
    """
    Insert a flat and its photo to the database.
    
    Args:
        flat: Flat data dictionary
        collections: Dictionary of collection references
        
    Returns:
        True if new flat inserted, False if duplicate
    """
    flat_with_id = dict(flat, **{"_id": int(flat["propertyCode"])})
    
    try:
        collections["flats"].insert_one(flat_with_id)
        
        # Download and store image if available
        if "thumbnail" in flat:
            image_bytes = get_image_idealista(flat["thumbnail"])
            if image_bytes:
                collections["photos"].insert_one({
                    "_id": int(flat["propertyCode"]),
                    "image": image_bytes
                })
        
        return True
    except errors.DuplicateKeyError:
        return False


def insert_price_record(
    flat_price: Dict[str, Any],
    collections: Dict[str, pymongo.collection.Collection],
    new_flat_ids: List[int]
) -> bool:
    """
    Insert price record or check if it already exists.
    
    Args:
        flat_price: Price data dictionary
        collections: Dictionary of collection references
        new_flat_ids: List of newly inserted flat IDs
        
    Returns:
        True if price is a new change, False if no price change
    """
    flat_price["price"] = int(flat_price["price"])
    
    query = {
        "propertyCode": flat_price["propertyCode"],
        "price": flat_price["price"]
    }
    
    # Check if exact price record already exists
    if collections["prices"].find_one(query):
        collections["prices_no_change"].insert_one(flat_price)
        return False
    else:
        collections["prices"].insert_one(flat_price)
        # Return True only if this is an existing property with price change
        return flat_price["propertyCode"] not in new_flat_ids


def save_new_flats(flats_info: List[Dict[str, Any]], output_key: str) -> None:
    """
    Save flat information to JSON file.
    
    Args:
        flats_info: List of flat dictionaries
        output_key: Key in OUTPUT_FILES dict
    """
    filepath = OUTPUT_FILES.get(output_key)
    if not filepath:
        logger.warning(f"Unknown output key: {output_key}")
        return
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(flats_info, f, ensure_ascii=False, indent=2)
    logger.info(f"✓ Saved {len(flats_info)} items to {filepath}")


def main(args) -> None:
    """
    Main function to scrape flats and update MongoDB.
    
    Args:
        args: Parsed command line arguments
    """
    logger.info(f"Starting with arguments: {args}")
    
    search_type = args_to_search_type(args)
    is_rent = search_type in (SearchType.RENT, SearchType.RENT_PENTHOUSE)
    
    # Fetch flats from API
    flats = get_flats(
        n_pages_x_request=args.pages,
        search_type=search_type
    )

    if not flats:
        logger.warning("⚠️ No flats scraped today")
        return

    logger.info(f"📊 Scraped {len(flats)} flats")

    # Get database and collections
    db = get_db("admin")
    collections = get_collections(db, is_rent)
    
    # Clear temporary output files
    for key in OUTPUT_FILES:
        save_new_flats([], key)
    
    # Insert flats and track new ones
    new_flats_count = 0
    old_flats_count = 0
    new_flats_ids = []
    new_flats_info = []

    for flat in flats:
        if add_flat_to_db(flat, collections):
            new_flats_count += 1
            new_flats_ids.append(flat["propertyCode"])
            new_flats_info.append(flat)
        else:
            old_flats_count += 1

    # Insert price records
    selected_keys = ["propertyCode", "date", "price"]
    flat_prices = [
        {k: v for k, v in flat.items() if k in selected_keys}
        for flat in flats
    ]
    
    price_changes = sum(
        1 for flat_price in flat_prices
        if insert_price_record(flat_price, collections, new_flats_ids)
    )

    logger.info(f"✅ Inserted: {new_flats_count} new, {old_flats_count} duplicates. Price changes: {price_changes}")

    # Save new flats to output files based on search type
    if search_type == SearchType.YOLO_PENTHOUSE:
        save_new_flats(new_flats_info, "newest_penthouses")
    elif search_type == SearchType.STANDARD:
        save_new_flats(new_flats_info, "newest_flats")       


if __name__ == "__main__":
    parser = ArgumentParser(description="Scrape flats from Idealista and store in MongoDB")
    parser.add_argument("--pages", type=int, default=20, choices=range(2, 2001),
                        help="Number of pages to scrape (2-2000)")
    parser.add_argument("--mab", action="store_true", help="Search for MAB properties")
    parser.add_argument("--house", action="store_true", help="Search for houses")
    parser.add_argument("--yolo_penthouse", action="store_true", help="Search for yolo penthouses")
    parser.add_argument("--rent", action="store_true", help="Search for rental properties")
    parser.add_argument("--rent_penthouse", action="store_true", help="Search for penthouse rentals")
    
    args = parser.parse_args()
    main(args)