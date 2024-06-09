import os
import requests
import telepot
import json

from src.model import find_terrace_sentence, find_terrace_size

token = os.environ.get('BOT_TOKEN')
chat_id = os.environ.get('CHAT_ID')

bot = telepot.Bot(token)

def get_newest_flats():
    with open("output/newest_flats.json", "r") as f:
        flats = json.load(f)

    print(len(flats))


 

# text = (
#     f"{title}"
# )



def send_message(text, photo_url):
   url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text 
   results = requests.get(url_req)
   print(results.json())
   bot.sendPhoto(chat_id, photo_url)


def main():
    # send_message("YOUR_MESSAGE")
    pass


if __name__ == "__main__":
    # main()
    get_newest_flats()