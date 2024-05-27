import pyrogram.errors
import requests
import os
import logging
import time
import re

import datetime

import config as cfg

logger = logging.getLogger("VK_API")
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s:%(levelname)s] --> %(message)s')


async def tg_get_group_posts(group, dt_begin, dt_end, client):

    new_dt_end = dt_end
    await client.start()
    try:
        if group in ('Hackathonlist', 'hackathons'):
            group = 'Hackathonlist'
            # logger.info('Hackathonlist')
            group = -1001169443045
        elif group.lower() == 'yandexacademy':
            group = -1001210388473
        elif group.lower() == 'mtsofficial':
            group = -1001111787841
        elif group.lower() == 'danone_careers':
            group = -1001639543716
        while True:
            try:
                async for message in client.get_chat_history(group, offset_date=dt_end):
                    if message.date.timestamp() <= dt_begin.timestamp():
                        break
                    else:
                        new_dt_end = message.date
                    time.sleep(1)
                    try:
                        while True:
                            try:
                                comments = await client.get_discussion_replies_count(message.chat.id, message.id)
                                # comments = '-'
                                break
                            except pyrogram.errors.FloodWait as ex:
                                logger.info(
                                    f'Telegram flood error while counting replies, sleeping for {ex.value} seconds')
                                time.sleep(ex.value)
                        yield message, comments
                    except pyrogram.errors.MsgIdInvalid:
                        yield message, '-'
                correct_finish = True
            except pyrogram.errors.exceptions.flood_420.FloodWait as ex:
                dt_end = new_dt_end
                logger.warning(f'{str(ex)}\nSleeping')

                logger.info(f'ex.args:{ex.args}') #TODO
                time.sleep(int(ex.value[0]))
                correct_finish = False
            except Exception as ex:
                logger.error(f'ERROR:\n{ex}')
                with open('error_links.txt', 'a') as f:
                    f.write(f't.me/{group}\n')
                correct_finish = False
                break
            if correct_finish:
                break
    except (pyrogram.errors.UsernameNotOccupied, KeyError, pyrogram.errors.UsernameInvalid) as ex:
        logger.warning(f'Error link: {ex}')
        with open('error_links.txt', 'a') as f:
            f.write(f't.me/{group}\n')
    await client.stop()
    time.sleep(13)


def vk_request_handler(link, query):
    cnt = 0
    while True:
        try:
            token = cfg.VK_TOKEN
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
            #if answer['error']['error_msg'] in ('Access denied: this wall available only for community members', 'Access denied: wall is disabled', 'Access denied: group is blocked'):
            return False
            logger.exception(f'Get vk_api error: {answer}')

    if not success:
        token = cfg.EXTRA_VK_TOKEN
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
                group.replace('id-', 'club')
                f.write(f'vk.com/{group}\n')
            break
        else:
            try:
                own_id = response['items'][0]['owner_id']
                if own_id == 716921852 or own_id == 84143893:
                    with open('error_links.txt', 'a') as f:
                        group.replace('id-', 'club')
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
