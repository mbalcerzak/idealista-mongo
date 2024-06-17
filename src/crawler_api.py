import base64
import requests
import json
import simplejson
from datetime import datetime


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


def today_str() -> str:
    return datetime.today().strftime('%Y-%m-%d')


class BasicParams:
    def __init__(self):
        self.locationId = "0-EU-ES-46"
        self.propertyType = "homes"
        self.order = "publicationDate"
        self.locale = "es"

class IdealistaParams(BasicParams):
    def __init__(self):
        super().__init__()
        self.minPrice = 1
        self.maxPrice = 9_999_999
        # self.minSize = 50
        self.maxSize = 350
        self.sort = "desc"
        self.locale = "es"
        self.operation = "sale"


class HouseParams(BasicParams):
    def __init__(self):
        super().__init__()
        self.minPrice = 1
        self.maxPrice = 300_000
        self.minSize = 90
        self.sort = "desc"
        self.locale = "es"
        self.chalet = "true"
        self.operation = "sale"


class MabParams(BasicParams):
    def __init__(self):
        super().__init__()
        self.penthouse = "true"
        # self.minSize = 70
        self.maxPrice = 300_000
        self.exterior = "true"
        self.hasLift = "true"
        self.operation = "sale"

class YoloPenthouse(IdealistaParams):
    def __init__(self):
        super().__init__()
        self.penthouse = "true"
        self.operation = "sale"
    
class DummyParams(BasicParams):
    def __init__(self):
        super().__init__()
        self.maxItems = 5
        self.numPage = 1

class RentParams(IdealistaParams):
    def __init__(self):
        super().__init__()
        self.operation = "rent"


class RentPenthouseParams(IdealistaParams):
    def __init__(self):
        super().__init__()
        self.operation = "rent"
        self.penthouse = "true"


def get_flats(
        save_json=True, 
        filename="data/scraped_api.json", 
        n_pages_x_request = 2000, 
        mab=False, 
        house=False, 
        yolo_penthouse=False,
        rent=False,
        rent_penthouse=False
        ):

    with open(".db_creds/idealista_cred.json", "r") as f:
        creds_all = json.load(f)

    today = today_str()
    result = None
    fresh_creds = None
    response = None

    dummy_params = DummyParams().__dict__

    for i, creds in enumerate(creds_all):
        print(f"({i+1}) {creds=}")
        try:
            idealista = Idealista(**creds)
            response = idealista.make_request("POST", dummy_params, country="es")

            if response:
                fresh_creds = creds
                break

        except (json.decoder.JSONDecodeError, simplejson.errors.JSONDecodeError):
            print(f"Creds {i+1} not working")
            continue 

    print(f"{fresh_creds=}")

    if fresh_creds:
        try:
            idealista = Idealista(**fresh_creds)
            data = []

            if mab:
                params = MabParams().__dict__
            elif house:
                params = HouseParams().__dict__
            elif yolo_penthouse:
                params = YoloPenthouse().__dict__
            elif rent:
                print("ARGS: rent")
                params = RentParams().__dict__
            elif rent_penthouse:
                print("ARGS: rent_penthouse")
                params = RentPenthouseParams().__dict__
            else:
                params = IdealistaParams().__dict__

            print(f"{params=}")
            
            for n_page in range(1, n_pages_x_request):
                params["numPage"] = n_page
                print(f"\tPage:{n_page}")

                req_result = idealista.make_request("POST", params, country="es")
                print(f"Number of flats: {len(data)}")
                elements = req_result["elementList"]

                data += elements

            if save_json:
                if len(data) > 0:
                    with open(filename, "w") as f:
                        result = [dict(item, **{'date':today}) for item in data]
                        json.dump(result, f)

            return result

        except:
            if save_json:
                if len(data) > 0:
                    with open(filename, "w") as f:
                        result = [dict(item, **{'date':today}) for item in data]
                        json.dump(result, f)  

            return result   


if __name__ == "__main__":
    flats = get_flats(n_pages_x_request = 2, mab=False, house=False)