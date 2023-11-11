import telebot
import configparser

config = configparser.ConfigParser()
config.read("config.ini", encoding="UTF-8")


def get_bot():
    token = config["Telegram"]["token"]
    return telebot.TeleBot(token=token, parse_mode="None")


def get_channel():
    return config["Telegram"]["channel"]


def send_telegram_error(message: str):
    try:
        use_telegram = config["Telegram"]["USE_TELEGRAM"]

        if use_telegram == "1":
            bot = get_bot()
            channel = get_channel()
            bot.send_message(channel, message)
    except KeyError as err:
        print("Required by Telegram-bot field is not found: \n\n", err)
        return
