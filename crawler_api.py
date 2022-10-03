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


def get_flats(n, save_json=True, filename="scraped_api.json"):

    # config = configparser.ConfigParser()
    # config.read('params.ini')

    n_pages_x_request = 2000  # number of pages to query
    max_items_per_request = 50 # max current support is 50
    min_price = 1
    max_price = 9999999
    minSize = 40
    order = "publicationDate"
    penthouse = "true"
    # num peticiones    = n_pages x n_cities

    with open("idealista_cred.json", "r") as f:
        creds_all = json.load(f)

    creds = creds_all[n]

    idealista = Idealista(**creds)

    today = today_str()

    try:
        data = []
        for n_page in range(1, n_pages_x_request):

            print(f"\tPage:{n_page}")
            params = {
                "operation": "sale",
                "locationId": "0-EU-ES-46",
                # "locationLevel": 6,
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

            req_result = idealista.make_request("POST", params, country="es")
            print(f"Number of flats: {len(data)}")
            elements = req_result["elementList"]
            data += elements

        if save_json:
            with open(filename, "w") as f:
                result = [dict(item, **{'date':today}) for item in data]
                json.dump(result, f)

        return result

    except:
        if save_json:
            with open(filename, "w") as f:
                result = [dict(item, **{'date':today}) for item in data]
                json.dump(result, f)  

        return result   


if __name__ == "__main__":
    flats = get_flats()