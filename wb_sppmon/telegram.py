"""
Function to send message to Telegram user or chat
"""
from . import helpers

URL_TELEGRAM_API = 'https://api.telegram.org/bot'


def send_to_telegram(token: str, chat_id: int | str, text: str):
    """
    Send a message to Telegram.
    Raises an exception if the send fails.
    Message must be valid Telegram-limited HTML, use html.escape(...).
    @param token: bot token
    @param chat_id: ID of the Telegram user or chat, can be in the form 'telegram:123456789'
    @param text: formatted message to send
    """
    if isinstance(chat_id, str):
        if chat_id.startswith('telegram:'):
            chat_id = int(chat_id[9:])

    url = f'{URL_TELEGRAM_API}{token}/sendMessage'
    data_json = {
        'chat_id': chat_id,
        'parse_mode': 'HTML',
        'text': text
    }
    http_headers = {
        'Content-Type': 'application/JSON; charset=utf-8'
    }
    helpers.http_request('POST', url, json=data_json, headers=http_headers)


def send_to_telegram_multiple(token: str, chat_ids: list[int | str], text: str) -> dict[str, Exception]:
    """
    Send a message to multiple Telegram recipients.
    Message must be valid Telegram-limited HTML, use html.escape(...).
    @return: chat_id => Exception, for all failed sends.
    """
    failures = {}
    for chat_id in chat_ids:
        try:
            send_to_telegram(token, chat_id, text)
        except Exception as e:
            failures[chat_id] = e

    return failures
