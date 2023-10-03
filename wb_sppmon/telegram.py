"""
Function to send message to Telegram user or chat
"""
from . import helpers

URL_TELEGRAM_API = 'https://api.telegram.org/bot'


def send_to_telegram(token: str, chat_id: int | str, text: str):
    """
    Send a message to Telegram
    @param token: bot token
    @param chat_id: ID of the Telegram user or chat, can be in the form 'telegram:123456789'
    @param text: message to send
    """
    if isinstance(chat_id, str):
        if chat_id.startswith('telegram:'):
            chat_id = int(chat_id[9:])

    url = f'{URL_TELEGRAM_API}{token}/sendMessage'
    data_json = {
        'chat_id': chat_id,
        'parse_mode': 'markdown',
        'text': text
    }
    http_headers = {
        'Content-Type': 'application/JSON; charset=utf-8'
    }
    helpers.http_request('POST', url, json=data_json, headers=http_headers)
