import logging
import telebot

from config import config_dict

if config_dict["TELEGRAM"]["TOKEN"]:
    BOT_TOKEN = config_dict["TELEGRAM"]["TOKEN"]
else:
    raise Exception("Telegram bot token is not provided.")

bot = telebot.TeleBot(BOT_TOKEN)


class TelegramBotHandler(logging.Handler):
    def __init__(self, chat_id):
        super().__init__()
        self.chat_id = chat_id

    def emit(self, record):
        log_entry = self.format(record)
        bot.send_message(chat_id=self.chat_id, text=log_entry)
