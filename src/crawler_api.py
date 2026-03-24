import base64
import requests
import json
import simplejson
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict


def today_str() -> str:
    """Return today's date as a formatted string."""
    return datetime.today().strftime('%Y-%m-%d')


@dataclass
class ParamConfig:
    """
    Configuration container for search parameters.
    
    All common parameters have defaults, but additional parameters can be passed
    via **kwargs and will be included in the output dictionary.
    """
    locationId: str = "0-EU-ES-46"
    propertyType: str = "homes"
    order: str = "publicationDate"
    locale: str = "es"
    minPrice: int = 1
    maxPrice: int = 9_999_999
    sort: str = "desc"
    operation: str = "sale"
    # Additional parameters (optional)
    maxSize: Optional[int] = None
    minSize: Optional[int] = None
    penthouse: Optional[str] = None
    exterior: Optional[str] = None
    hasLift: Optional[str] = None
    chalet: Optional[str] = None
    maxItems: Optional[int] = None
    numPage: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class SearchType(Enum):
    """Enumeration of available search parameter presets."""
    STANDARD = "standard"
    TERRAIN = "terrain"
    HOUSE = "house"
    MAB = "mab"
    YOLO_PENTHOUSE = "yolo_penthouse"
    RENT = "rent"
    RENT_PENTHOUSE = "rent_penthouse"
    DUMMY = "dummy"


class ParamRegistry:
    """
    Registry for search parameter configurations.
    Provides a centralized, extensible way to manage different search presets.
    """
    
    _configs: Dict[SearchType, ParamConfig] = {}
    
    @classmethod
    def register(cls, search_type: SearchType, config: ParamConfig) -> None:
        """Register a new parameter configuration."""
        cls._configs[search_type] = config
    
    @classmethod
    def get(cls, search_type: SearchType) -> ParamConfig:
        """Retrieve a parameter configuration by type."""
        if search_type not in cls._configs:
            raise ValueError(f"Unknown search type: {search_type.value}")
        return cls._configs[search_type]
    
    @classmethod
    def list_available(cls) -> Dict[str, str]:
        """List all available search types."""
        return {st.value: st.name for st in cls._configs.keys()}


# Register default parameter configurations
def _initialize_param_registry():
    """Initialize the parameter registry with default configurations."""
    
    ParamRegistry.register(
        SearchType.STANDARD,
        ParamConfig(
            minPrice=1,
            maxPrice=9_999_999,
            maxSize=350,
            operation="sale",
        )
    )
    
    ParamRegistry.register(
        SearchType.TERRAIN,
        ParamConfig(
            minPrice=1,
            maxPrice=400_000,
            propertyType="land",
            operation="sale",
        )
    )
    
    ParamRegistry.register(
        SearchType.HOUSE,
        ParamConfig(
            minPrice=1,
            maxPrice=300_000,
            minSize=90,
            chalet="true",
            operation="sale",
        )
    )
    
    ParamRegistry.register(
        SearchType.MAB,
        ParamConfig(
            penthouse="true",
            maxPrice=300_000,
            exterior="true",
            hasLift="true",
            operation="sale",
        )
    )
    
    ParamRegistry.register(
        SearchType.YOLO_PENTHOUSE,
        ParamConfig(
            minPrice=1,
            maxPrice=9_999_999,
            maxSize=350,
            penthouse="true",
            operation="sale",
        )
    )
    
    ParamRegistry.register(
        SearchType.RENT,
        ParamConfig(
            minPrice=1,
            maxPrice=9_999_999,
            maxSize=350,
            operation="rent",
        )
    )
    
    ParamRegistry.register(
        SearchType.RENT_PENTHOUSE,
        ParamConfig(
            minPrice=1,
            maxPrice=9_999_999,
            maxSize=350,
            penthouse="true",
            operation="rent",
        )
    )
    
    ParamRegistry.register(
        SearchType.DUMMY,
        ParamConfig(
            maxItems=5,
            numPage=1,
        )
    )


# Initialize registry on module import
_initialize_param_registry()


class Idealista:
    def __init__(self, api_key, secret):
        self.api_key = api_key
        self.secret = secret
        self.base64 = base64.b64encode(
            f"{self.api_key}:{self.secret}".encode()
        ).decode()
        self.access_token = self.__get_access_token()

    def __str__(self) -> str:
        return f"API KEY {self.api_key}  \nSecret: {self.secret} \nBase64: {self.base64}\nAccess token: {self.access_token}"

    def __get_access_token(self):
        api_headers = {
            "Authorization": f"Basic {self.base64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        return requests.post(
            url="https://api.idealista.com/oauth/token",
            data="grant_type=client_credentials&scope=read",
            headers=api_headers,
        ).json()["access_token"]

    def make_request(self, kind: str, params: dict, country: str) -> str:

        if kind == "GET":
            pass
        elif kind == "POST":
            headers_dic = {
                "Authorization": "Bearer " + self.access_token,
                "Content-Type": "application/x-www-form-urlencoded",
            }

            return requests.post(
                url=f"https://api.idealista.com/3.5/{country}/search",
                headers=headers_dic,
                params=params,
            ).json()

        else:
            raise ValueError(f"{kind} no es un tipo valido de petición")


def get_flats(
        save_json: bool = True,
        filename: str = "data/scraped_api.json",
        n_pages_x_request: int = 2000,
        search_type: SearchType = SearchType.STANDARD,
):
    """
    Fetch flats data from Idealista API.
    
    Args:
        save_json: Whether to save results to a JSON file
        filename: Path to save JSON output
        n_pages_x_request: Number of pages to fetch
        search_type: Type of search parameters to use (from SearchType enum)
        
    Returns:
        List of flat dictionaries with date field added, or None if no results
        
    Raises:
        ValueError: If invalid search_type or no working credentials found
    """
    with open(".db_creds/idealista_cred.json", "r") as f:
        creds_all = json.load(f)

    today = today_str()
    result = None
    fresh_creds = None
    response = None

    # Test credentials with dummy params
    dummy_params = ParamRegistry.get(SearchType.DUMMY).to_dict()

    for i, creds in enumerate(creds_all):
        print(f"({i+1}) Testing credentials: {creds.get('api_key', 'unknown')[:8]}...")
        try:
            idealista = Idealista(**creds)
            response = idealista.make_request("POST", dummy_params, country="es")

            if response:
                fresh_creds = creds
                print(f"✓ Credentials valid (attempt {i+1})")
                break

        except (json.decoder.JSONDecodeError, simplejson.errors.JSONDecodeError):
            print(f"✗ Credentials attempt {i+1} failed")
            continue

    if not fresh_creds:
        raise ValueError("No valid credentials found in idealista_cred.json")

    try:
        idealista = Idealista(**fresh_creds)
        data = []
        
        # Get parameters for the specified search type
        params = ParamRegistry.get(search_type).to_dict()
        print(f"\n📍 Search type: {search_type.value}")
        print(f"🔍 Parameters: {params}\n")
        
        # Fetch pages
        for n_page in range(1, n_pages_x_request):
            params["numPage"] = n_page
            print(f"Fetching page {n_page}...", end=" ")

            req_result = idealista.make_request("POST", params, country="es")
            elements = req_result.get("elementList", [])
            data.extend(elements)
            print(f"(+{len(elements)} items, total: {len(data)})")

        # Save results
        if save_json and data:
            with open(filename, "w", encoding="utf-8") as f:
                result = [dict(item, **{'date': today}) for item in data]
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n✅ Saved {len(result)} items to {filename}")
        
        return result if data else None

    except Exception as e:
        print(f"\n❌ Error during fetch: {e}")
        
        # Save partial results on error
        if save_json and data:
            with open(filename, "w", encoding="utf-8") as f:
                result = [dict(item, **{'date': today}) for item in data]
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"⚠️ Saved {len(result)} partial items to {filename}")
        
        return result if data else None


# Backwards compatibility wrapper
def get_flats_legacy(
        save_json: bool = True,
        filename: str = "data/scraped_api.json",
        n_pages_x_request: int = 2000,
        mab: bool = False,
        house: bool = False,
        yolo_penthouse: bool = False,
        rent: bool = False,
        rent_penthouse: bool = False,
):
    """
    Legacy interface for get_flats() for backwards compatibility.
    Maps boolean flags to SearchType enum.
    
    DEPRECATED: Use get_flats() with search_type parameter instead.
    """
    # Determine search type from flags
    if mab:
        search_type = SearchType.MAB
    elif house:
        search_type = SearchType.HOUSE
    elif yolo_penthouse:
        search_type = SearchType.YOLO_PENTHOUSE
    elif rent:
        search_type = SearchType.RENT
    elif rent_penthouse:
        search_type = SearchType.RENT_PENTHOUSE
    else:
        search_type = SearchType.STANDARD

    return get_flats(
        save_json=save_json,
        filename=filename,
        n_pages_x_request=n_pages_x_request,
        search_type=search_type,
    )   


if __name__ == "__main__":
    # Example: Standard search
    # flats = get_flats(n_pages_x_request=2, search_type=SearchType.STANDARD)
    
    # Example: MAB search
    # flats = get_flats(n_pages_x_request=2, search_type=SearchType.MAB)
    
    # Example: Legacy API (still supported)
    flats = get_flats_legacy(n_pages_x_request=2, mab=False, house=False)