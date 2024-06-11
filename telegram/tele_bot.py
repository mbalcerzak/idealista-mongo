import os
import requests
import telepot
import json
import re
from argparse import ArgumentParser

from src.preprocessing import get_terrace_from_description, \
            find_terrace_size, get_balcon, get_terrace_yn

token = os.environ.get('BOT_TOKEN')
chat_id = os.environ.get('CHAT_ID')

bot = telepot.Bot(token)


def load_newest_flats(penthouse=False):
    if penthouse:
        with open("output/newest_penthouses.json", "r") as f:
            return json.load(f)
    else:        
        with open("output/newest_flats.json", "r") as f:
            return json.load(f)


def get_newest_flats() -> list:
    """Picks flats according to requirments from the onces recently scraped"""
    flats = load_newest_flats()

    print(f"{len(flats)} New flats today")

    cols = ["neighborhood", "thumbnail", "url",  "hasLift", "floor", "price", "size", 
            "propertyType", "rooms", "bathrooms", "newDevelopment", "exterior"]

    terrace_flats = []

    for flat in flats:
        if flat["municipality"] == "València":
            if flat["price"] < 300000:
                terrace_str = get_terrace_from_description(flat["description"])
                terrace_size = find_terrace_size(terrace_str)
                terrace_yn = get_terrace_yn(flat["description"])
                balcon = get_balcon(flat["description"])

                if terrace_str or terrace_yn or balcon:
                    flat_new = {k:v for k, v in flat.items() if k in cols}
                    flat_new['terraceSize'] = terrace_size
                    flat_new["terrace_str"] = terrace_str
                    flat_new["hasTerrace"] = terrace_yn
                    flat_new['balcon'] = balcon

                    flat_new['title'] = flat["suggestedTexts"]["title"]
                    flat_new['title'] = re.sub(r'[^\w\s]', ' ', flat_new['title'])

                    terrace_flats.append(flat_new)

    return terrace_flats
 

def get_penthouses(penthouse) -> list:
    """Picks flats according to requirments from the onces recently scraped"""
    flats = load_newest_flats(penthouse)

    print(f"{len(flats)} New penthouses today")

    cols = ["neighborhood", "thumbnail", "url",  "hasLift", "floor", "price", "size", 
            "propertyType", "rooms", "bathrooms", "newDevelopment", "exterior"]

    terrace_flats = []

    for flat in flats:
        if flat["municipality"] == "València":
            terrace_str = get_terrace_from_description(flat["description"])
            terrace_size = find_terrace_size(terrace_str)
            flat_new = {k:v for k, v in flat.items() if k in cols}
            flat_new['terraceSize'] = terrace_size
            flat_new["terrace_str"] = terrace_str
            terrace_yn = get_terrace_yn(flat["description"])
            flat_new["hasTerrace"] = terrace_yn

            balcon = get_balcon(flat["description"])
            if balcon:
                flat_new['balcon'] = True

            flat_new['title'] = flat["suggestedTexts"]["title"]
            flat_new['title'] = re.sub(r'[^\w\s]', ' ', flat_new['title'])

            terrace_flats.append(flat_new)

    return terrace_flats


def format_message(flat: dict) -> str:
    info_cols = ['propertyType', 'price', 'neighborhood', 'size', "hasTerrace", 'terrace_str', 'terraceSize',
                'floor', 'exterior', 'rooms', 'bathrooms', 'newDevelopment', 'hasLift']
    
    common_cols = [i for i in info_cols if i in flat.keys()]

    formatted_dict = '\n'.join([f"{key}: {flat[key]}" for key in common_cols])
    # Markdown has a meltdown when special character "." is used so we're removing it
    formatted_dict = formatted_dict.replace(".0","").replace('-', '')

    text = (f"""*__{flat['title']}__*\n{'x'*80}\n\n{formatted_dict} \n\n[Idealista link]({flat['url']})""")

    return text


def send_message(text):
   bot.sendMessage(chat_id, text, parse_mode='MarkdownV2')


def main(args):
    print(args.__dict__)
    penthouse = args.penthouse

    if penthouse:
        new_flats = get_penthouses(penthouse)
        print("PENTHOUSE")
    else:
        new_flats = get_newest_flats(penthouse)
        print("ALL FLATS")

    for flat in new_flats:
        print(flat)
        text = format_message(flat)

        print(text)
        # send_message(text)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-penthouse', '--penthouse', action="store_true")
    args = parser.parse_args()

    main(args)