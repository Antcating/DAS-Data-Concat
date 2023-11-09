import telebot
import configparser

config = configparser.ConfigParser()
config.read("config.ini", encoding="UTF-8")

token = config["Telegram"]["token"]
error_channel = config["Telegram"]["channel"]

bot = telebot.TeleBot(token=token, parse_mode="None")


def send_telegram_error(message: str):
    bot.send_message(error_channel, message)
