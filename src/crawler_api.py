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


def get_flats(save_json=True, filename="data/scraped_api.json", n_pages=2000):

    n_pages_x_request = n_pages  # number of pages to query
    max_items_per_request = 50 # max current support is 50
    min_price = 1
    max_price = 9999999
    minSize = 40
    order = "publicationDate"
    penthouse = "true"

    with open(".db_creds/idealista_cred.json", "r") as f:
        creds_all = json.load(f)

    # fresh_creds = creds_all[3]

    today = today_str()
    result = None
    fresh_creds = None

    response = None
    dummy_params = {
        "locationId": "0-EU-ES-46", 
        "operation": "sale", 
        "propertyType": "homes", 
        "numPage": 1
        }
    
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

                params = {
                    "operation": "sale",
                    "locationId": "0-EU-ES-46",
                    "propertyType": "homes",
                    "penthouse": penthouse,
                    "locale": "es",
                    "maxItems": max_items_per_request,
                    "maxPrice": max_price,
                    "minPrice": min_price,
                    "minSize": minSize,
                    "numPage": n_page,
                    "order": order,
                    "sort": "desc"
                }

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