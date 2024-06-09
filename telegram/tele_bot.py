import os
import requests
import telepot
import json

from src.model import find_terrace_sentence, find_terrace_size, get_balcon

token = os.environ.get('BOT_TOKEN')
chat_id = os.environ.get('CHAT_ID')

bot = telepot.Bot(token)


def load_newest_flats():
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
            terrace_str = find_terrace_sentence(flat["description"])
            terrace_size = find_terrace_size(terrace_str)

            if terrace_str:
                flat_new = {k:v for k, v in flat.items() if k in cols}
                flat_new['terraceSize'] = terrace_size

                balcon = get_balcon(flat["description"])
                if balcon:
                    flat_new['balcon'] = True

                flat_new['title'] = flat["suggestedTexts"]["title"]

                terrace_flats.append(flat_new)

    return terrace_flats
 

def format_message(flat: dict) -> str:
    info_cols = ['propertyType', 'price', 'neighborhood', 'size', 'terraceSize',
                'floor', 'exterior', 'rooms', 'bathrooms', 'newDevelopment', 'hasLift']

    formatted_dict = '\n'.join([f"{key}: {flat[key]}" for key in info_cols])
    # Markdown has a meltdown when special character "." is used so we're removing it
    formatted_dict = formatted_dict.replace(".0","")

    text = (f"""{'_'*100}\n*\n*||ELO MORDO TEST TEST\\!\\!1||\n*\n*\n*__{flat['title']}__* \n\n{formatted_dict} \n\n[Idealista link]({flat['url']})""")

    return text


def send_message(text):
   bot.sendMessage(chat_id, text, parse_mode='MarkdownV2')


def main():
    new_flats = get_newest_flats()

    for flat in new_flats:
        text = format_message(flat)

        print(text)
        # send_message(text)


if __name__ == "__main__":
    main()