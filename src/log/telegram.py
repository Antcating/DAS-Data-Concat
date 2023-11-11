from contextlib import nullcontext
import telebot
import configparser

config = configparser.ConfigParser()
config.read("config.ini", encoding="UTF-8")


def get_bot():
    """Creates Telegram bot to be used

    Returns:
        telebot.bot: Returns created Telegram bot instance
    """
    try:
        token = config["Telegram"]["token"]
        bot = telebot.TeleBot(token=token, parse_mode="None")
    except KeyError:
        bot = nullcontext
        print("Bot token is not provided")
    return bot


def get_channel():
    """Returns required channel to send errors to

    Returns:
        str: Channel username/id
    """
    try:
        channel = config["Telegram"]["channel"]
    except KeyError:
        print("Channel username/id is required to use Telegram bot")
        channel = None
    return channel


def send_telegram_error(message: str):
    try:
        use_telegram = config["Telegram"]["USE_TELEGRAM"]

        if use_telegram == "1":
            bot = get_bot()
            channel = get_channel()
            if bot and channel:
                bot.send_message(channel, message)
    except KeyError:
        print("Telegram status is uncertain or not provided")
        return
