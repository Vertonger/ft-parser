import pyrogram.errors
import requests
import os
import logging
import time
import re
from pyrogram import Client

logger = logging.getLogger("VK_API")
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s:%(levelname)s] --> %(message)s')


async def tg_get_group_posts(group, dt_begin, dt_end):
    api_id = os.environ['API_ID']  # https://my.telegram.org/auth
    api_hash = os.environ['API_HASH']
    client = Client('session', int(api_id), api_hash, phone_number=os.environ['TELEGRAM_NUMBER'])

    await client.start()
    try:
        async for message in client.get_chat_history(group, offset_date=dt_end):
            if message.date.timestamp() <= dt_begin.timestamp():
                break
            try:
                while True:
                    try:
                        comments = await client.get_discussion_replies_count(message.chat.id, message.id)
                        break
                    except pyrogram.errors.FloodWait:
                        logger.info('Telegram flood error, sleeping for 2 minutes')
                        time.sleep(120)
                yield message, comments
            except pyrogram.errors.MsgIdInvalid:
                yield message, 0
    except pyrogram.errors.UsernameNotOccupied:
        with open('error_links.txt', 'a') as f:
            f.write(f't.me/{group}\n')
    await client.stop()
    time.sleep(13)


def vk_request_handler(link, query):
    cnt = 0
    while True:
        try:
            token = os.environ[f'VK_TOKEN_{cnt}']
        except KeyError:
            success = False
            break
        cnt += 1
        query['access_token'] = token
        answer = requests.post(link, data=query).json()
        try:
            response = answer['response']
            success = True
            break
        except KeyError:
            logger.exception(f'Get vk_api error: {answer}')

    if not success:
        token = os.environ['EXTRA_VK_TOKEN']
        query['access_token'] = token
        answer = requests.post(link, data=query).json()
        try:
            response = answer['response']
        except KeyError:
            logger.exception(f'Get vk_api error: {answer}')
            return False
    return response


def vk_get_group_posts(group, offset=0):
    link = 'https://api.vk.com/method/wall.get?'
    while True:
        if re.fullmatch(r'public\d+', group):
            group = f'id-{group[6:]}'.replace('/', '')
        if re.fullmatch(r'club\d+', group):
            group = f'id-{group[4:]}'.replace('/', '')
        query = {
            'domain': str(group),
            'count': '100',
            'offset': str(offset),
            'extended': '0',
            'filter': 'owner',
            'v': '5.131'}
        response = vk_request_handler(link, query)
        if not response:
            with open('error_links.txt', 'a') as f:
                f.write(f'vk.com/{group}\n')
            break
        else:
            try:
                if response['items'][0]['owner_id'] == os.environ['VK_ID']:
                    with open('error_links.txt', 'a') as f:
                        f.write(f'vk.com/{group}\n')
                    break
            except (KeyError, IndexError):
                pass

        if not response['next_from']:
            offset = 0
        else:
            offset = int(response['next_from'])

        for post in response['items']:
            yield post

        if not offset:
            break


def vk_get_video_player(video):
    link = 'https://api.vk.com/method/video.get?'
    query = {
        'videos': video,
        'v': '5.131'}
    response = vk_request_handler(link, query)
    if response:
        try:
            return response['items'][0]['player']
        except (IndexError, KeyError):
            logger.info(f"Can't get vk video player, response: {response}")
            try:
                return response['items'][0]['restriction']['title']
            except IndexError:
                return False
    else:
        return False
