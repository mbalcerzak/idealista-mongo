import base64
import os
import requests
import json
import configparser
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


class IdealistaParams:
    def __init__(self):
        self.locationId = "0-EU-ES-46"
        self.operation = "sale"
        self.propertyType = "homes"
        self.maxItems = 50
        self.minPrice = 1
        self.maxPrice = 9999999
        self.minSize = 40
        self.order = "publicationDate"
        self.sort = "desc"
        self.locale = "es"


class DummyParams(IdealistaParams):
    def __init__(self):
        super().__init__()
        self.maxItems=1


class MabParams(IdealistaParams):
    def __init__(self):
        super().__init__()
        self.penthouse = "true"
        self.minSize = 50
        self.maxPrice = 260_000
        self.exterior = "true"
        self.hasLift = "true"



def get_flats(save_json=True, filename="data/scraped_api.json", n_pages_x_request = 2000, search="standard"):


    # 

    with open(".db_creds/idealista_cred.json", "r") as f:
        creds_all = json.load(f)

    today = today_str()
    result = None
    fresh_creds = None

    response = None
    dummy_params = DummyParams()
    
    for i, creds in enumerate(creds_all):
        print(f"({i+1}) {creds=}")
        try:
            idealista = Idealista(**creds)
            response = idealista.make_request("POST", dummy_params, country="es")

            if response:
                fresh_creds = creds
                break

        except json.decoder.JSONDecodeError:
            print(f"Creds {i+1} not working")
            continue

    if fresh_creds:
        try:
            idealista = Idealista(**fresh_creds)
            data = []

            for n_page in range(1, n_pages_x_request):

                params = IdealistaParams()
                params["n_page"] = n_page

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
    flats = get_flats(n_pages=5)