import api_methods as api
import datetime
import time
import logging
import os

import pyexcel
from aiogram import Bot, Dispatcher, executor, types

logger = logging.getLogger("MAIN")
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s:%(levelname)s] --> %(message)s')


def get_groups(file):
    with open(file, 'r') as f:
        all_text = f.read()
        if all_text.count('t.me'):
            social = 'tg'
            all_text = all_text.replace('t.me', '')
        elif all_text.count('vk.com'):
            social = 'vk'
            all_text = all_text.replace('vk.com/', '')
        else:
            social = 0
        result = all_text.replace('https://', '').replace('wall-', '').replace('/', '').split()
    return result, social


bot = Bot(token=os.environ['TELEBOT_TOKEN'])
dp = Dispatcher(bot)

allowed_chats = [218556652, 7345558, 521660043]
parsing = False
waiting_users = set()


@dp.message_handler(commands="start")
@dp.message_handler(commands="help")
async def handle_start(message: types.Message):
    if message.chat.id in allowed_chats:
        await bot.send_message(message.chat.id, 'Для начала парсинга отправьте боту файл формата "txt",'
                                                'где каждая строка соответствует группе вк, а период парсинга напишите '
                                                'в том же сообщении (!) в формате "ДД.ММ.ГГГГ-ДД.ММ.ГГГГ"')
    return


@dp.message_handler(commands=['cancel'])
async def handle_cancel(message):
    global parsing
    if message.chat.id not in allowed_chats:
        return
    if parsing == message.chat.id:
        parsing = False
        # markup = types.ReplyKeyboardRemove()
        await bot.send_message(message.chat.id, 'Парсинг отменён')
    elif not parsing:
        await bot.send_message(message.chat.id, 'Я и так не собираю никаких данных')
    else:
        await bot.send_message(message.chat.id, 'Я пока собираю данные для другого пользователя\nПридётся подождать')
    return


@dp.message_handler(content_types=types.ContentTypes.DOCUMENT)
async def handle_docs_photo(message):
    if message.chat.id == 5167706845:
        doc_id = message.document.file_id
        await bot.send_document(int(message.caption), doc_id)
    elif message.chat.id in allowed_chats:
        global parsing
        if parsing:
            global waiting_users
            waiting_users.add(message.chat.id)
            await bot.send_message(message.chat.id,
                                   'Я пока собираю данные для другого пользователя\nПридётся подождать')
            return
        try:
            data_period = message.caption.replace(' ', '')
        except ValueError:
            await bot.send_message(message.chat.id, 'В одном сообщении должен быть файл со ссылками и период парсинга')
            return
        try:
            period_begin_str, period_end_str = data_period.split('-')
            datetime.datetime.strptime(period_begin_str, '%d.%m.%Y')
            datetime.datetime.strptime(period_end_str, '%d.%m.%Y')

            file_name = message.document.file_name
            # file_id = message.document.file_id
            if file_name[-4:] != '.txt':
                await bot.send_message(message.chat.id,
                                       'Файл с ссылками должен быть в формате "txt" и каждая ссылка должна быть с '
                                       'новой строки')
                return

            file_info = await bot.get_file(message.document.file_id)
            downloaded_file = await bot.download_file(file_info.file_path)
            with open(file_name, 'wb') as new_file:
                new_file.write(downloaded_file.getvalue())

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add(types.KeyboardButton(text="Отмена ❌"))
            await bot.send_message(message.chat.id, 'Начинаю сбор данных', reply_markup=markup)
            parsing = message.chat.id
            await parsing_func(message.chat.id, file_name, period_begin_str, period_end_str)
        except (IndexError, ValueError):
            await bot.send_message(message.chat.id, 'Неправильно задан период\nПериод наобходимо задавать в формате '
                                                    '"ДД.ММ.ГГГГ-ДД.ММ.ГГГГ" (например, "01.02.2020-29.02.2020" для '
                                                    'февраля 2020 года)')
    return


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def text(message):
    logger.info(f'Received message from {message.chat.id}: {message.text}')
    if message.chat.id not in allowed_chats:
        await bot.send_message(message.chat.id, 'Это приватный бот.')
        return
    if message.text == 'Отмена ❌':
        await handle_cancel(message)
    return


async def parsing_func(chat_id, group_file, period_begin_str, period_end_str):
    global parsing
    global waiting_users
    groups, social = get_groups(group_file)

    start_time = time.time()
    group_cnt = 0
    total_post_cnt = 0
    time_for_group = []
    with open('error_links.txt', 'w') as f:
        f.write("Некорректные ссылки:\n")

    if social == 'vk':
        period_begin = time.mktime((datetime.datetime.strptime(period_begin_str, '%d.%m.%Y')
                                    - datetime.timedelta(seconds=60 * 60 * 3)).timetuple())
        logger.info(f'Parsing period start in utc: {period_begin}')
        period_end = time.mktime((datetime.datetime.strptime(period_end_str, '%d.%m.%Y')
                                  + datetime.timedelta(seconds=86399 - 60 * 60 * 3)).timetuple())
        logger.info(f'Parsing period end in utc: {period_end}')

        curr_mess = await bot.send_message(chat_id, 'Начинаю сбор инфорамции для групп ВКонтакте', parse_mode='HTML')

        all_posts_data = [
            ['Группа', 'Пост', 'Текст оригинального поста', 'Дата-время', 'Лайки', 'Репосты', 'Комментарии',
             'Просмотры', 'Фото', 'Видео', 'Текст репоста',
             'Ссылка на оригинальный пост (если репост)', 'Использованные форматы']]

        for vk_group in groups:
            if not parsing:
                return
            cycle_start_time = time.time()
            group_cnt += 1
            logger.info(f'Parsing vk.com/{vk_group}: {group_cnt}/{len(groups)}')
            if len(time_for_group) == 0:
                info_text = f'Обработано групп:<b>\n{group_cnt}/{len(groups)}</b>'
            else:
                average_time = round((sum(time_for_group) / (group_cnt - 1) * (len(groups) - group_cnt)) / 60, 1)
                if average_time < 1:
                    info_text = f'Обработано групп:<b>\n{group_cnt}/{len(groups)}</b>\n\nПочти закончил'
                else:
                    info_text = f'Обработано групп:<b>\n{group_cnt}/{len(groups)}</b>\n\n' \
                                f'Примерно осталось: \n{average_time} минут'

            await bot.edit_message_text(chat_id=chat_id, message_id=curr_mess.message_id,
                                        text=info_text, parse_mode='HTML')

            end_founded = False
            posts_cnt = 0
            for post in api.vk_get_group_posts(vk_group):
                total_post_cnt += 1
                posts_cnt += 1
                if posts_cnt % 100 == 0:
                    logger.info(f'Parsing {posts_cnt} post')
                try:
                    if post['is_pinned'] == 1:
                        pinned = True
                    else:
                        pinned = False
                except KeyError:
                    pinned = False

                if not end_founded:
                    if period_end >= post['date']:
                        if not pinned:
                            logger.info('End of period founded')
                            end_founded = True
                        elif period_begin >= post['date']:
                            continue

                    else:
                        continue
                if period_begin >= post['date']:
                    if not pinned:
                        break
                post_data = ['-'] * len(all_posts_data[0])

                # ссылка на группу
                post_data[0] = f'vk.com/wall{post["owner_id"]}'

                # ссылка на пост
                post_data[1] = f'vk.com/wall{post["owner_id"]}_{post["id"]}'

                # создание объекта оригинального поста в случае перепоста
                try:
                    orig_post = post['copy_history'][-1]
                except KeyError:
                    orig_post = False

                # текст поста, либо текст оригинального поста, если это репост
                if orig_post:
                    post_data[2] = orig_post['text']
                else:
                    post_data[2] = post['text']

                # дата-время
                post_data[3] = datetime.datetime.utcfromtimestamp(post['date'] + 3600 * 3).strftime('%Y.%m.%d %H:%M:%S')

                # лайки
                post_data[4] = post['likes']['count']

                # репосты
                post_data[5] = post['reposts']['count']

                # комментарии
                try:
                    post_data[6] = post['comments']['count']
                except KeyError:
                    pass

                # просмотры
                try:
                    post_data[7] = post['views']['count']
                except KeyError:
                    pass

                # фоты-видевы
                if orig_post:
                    try:
                        attachments = orig_post['attachments']
                    except KeyError:
                        attachments = False
                else:
                    try:
                        attachments = post['attachments']
                    except KeyError:
                        attachments = False
                if attachments:
                    photos = []
                    videos = []
                    polls = False

                    for attachment in attachments:
                        if attachment['type'] == 'photo':
                            photos.append(attachment['photo']['sizes'][-1]['url'])
                        elif attachment['type'] == 'video':
                            video = attachment['video']
                            try:
                                if 'access_key' in video:
                                    video_player = api.vk_get_video_player('_'.join([str(video['owner_id']),
                                                                                     str(video['id']),
                                                                                     video['access_key']]))
                                else:
                                    video_player = api.vk_get_video_player('_'.join([str(video['owner_id']),
                                                                                     str(video['id'])]))
                            except KeyError:
                                video_player = False
                            if not video_player:
                                await bot.send_message(chat_id,
                                                       f'Произошла ошибка с сохранением ссылки на видео из поста '
                                                       f'{post_data[1]}\nПожалуйста, оповестите об этом @Vertonger')
                            else:
                                videos.append(video_player)
                        elif attachment['type'] == 'poll':
                            polls = True

                    attachments = []
                    if len(photos) > 0:
                        post_data[8] = ', '.join(photos)
                        attachments.append('Фото')
                    if len(videos) > 0:
                        post_data[9] = ', '.join(videos)
                        attachments.append('Видео')
                    if polls:
                        attachments.append('Опрос')

                    if len(attachments) > 0:
                        post_data[12] = ', '.join(attachments)

                # текст репоста (если есть)
                if orig_post:
                    post_data[10] = post['text']

                    # ссылка на оригинал, если репост
                    post_data[11] = f'vk.com/wall{orig_post["owner_id"]}_{orig_post["id"]}'

                all_posts_data.append(post_data)
            time_for_group.append(time.time() - cycle_start_time)
    elif social == 'tg':
        period_begin = datetime.datetime.strptime(period_begin_str, '%d.%m.%Y') - \
                       datetime.timedelta(seconds=60 * 60 * 3)
        logger.info(f'Parsing period start: {period_begin.strftime("%Y-%m-%d %H:%M")}')
        period_end = datetime.datetime.strptime(period_end_str, '%d.%m.%Y') + \
                     datetime.timedelta(seconds=86399 - 60 * 60 * 3)
        logger.info(f'Parsing period end in utc: {period_end.strftime("%Y-%m-%d %H:%M")}')

        curr_mess = await bot.send_message(chat_id, 'Начинаю сбор инфорамции для каналов Telegram', parse_mode='HTML')

        all_posts_data = [
            ['Группа', 'Пост', 'Текст', 'Дата-время', 'Реакции', 'Комментарии',
             'Просмотры', 'Проголосовало в опросе', 'media_group_id', 'Фото', 'Видео', 'Использованные форматы']]

        for tg_group in groups:
            if not parsing:
                return
            cycle_start_time = time.time()
            group_cnt += 1
            logger.info(f'Parsing t.me/{tg_group}: {group_cnt}/{len(groups)}')
            if len(time_for_group) == 0:
                info_text = f'Обработано каналов:<b>\n{group_cnt}/{len(groups)}</b>'
            else:
                average_time = round((sum(time_for_group) / (group_cnt - 1) * (len(groups) - group_cnt)) / 60, 1)
                if average_time < 1:
                    info_text = f'Обработано каналов:<b>\n{group_cnt}/{len(groups)}</b>\n\nПочти закончил'
                else:
                    info_text = f'Обработано каналов:<b>\n{group_cnt}/{len(groups)}</b>\n\n' \
                                f'Примерно осталось: \n{average_time} минут'

            await bot.edit_message_text(chat_id=chat_id, message_id=curr_mess.message_id,
                                        text=info_text, parse_mode='HTML')

            end_founded = False
            posts_cnt = 0
            async for post, comments_count in api.tg_get_group_posts(tg_group, period_begin, period_end):
                total_post_cnt += 1
                posts_cnt += 1
                if posts_cnt % 100 == 0:
                    logger.info(f'Parsing {posts_cnt} post')

                post_data = ['-'] * len(all_posts_data[0])

                # ссылка на группу
                post_data[0] = f't.me/{tg_group}'

                # ссылка на пост
                post_data[1] = f't.me/{tg_group}/{post.id}'

                # текст поста
                if post.caption is None:
                    post_data[2] = post.text
                else:
                    post_data[2] = post.caption

                # дата-время
                post_data[3] = post.date.strftime("%Y-%m-%d %H:%M:%S")

                # реакции
                if post.reactions is not None:
                    post_data[4] = 0
                    for reaction in post.reactions:
                        post_data[4] += reaction.count

                # комментарии
                post_data[5] = comments_count

                # просмотры
                post_data[6] = post.views

                # media_group_id
                if post.media_group_id is not None:
                    post_data[8] = post.media_group_id

                # использованные форматы
                attachments = []
                if post.photo is not None:
                    attachments.append('Фото')
                if post.video is not None:
                    attachments.append('Видео')
                if post.poll is not None:
                    attachments.append('Опрос')
                    post_data[7] = post.poll.total_voter_count

                if len(attachments) > 0:
                    post_data[11] = ', '.join(attachments)

                all_posts_data.append(post_data)
            time_for_group.append(time.time() - cycle_start_time)
    else:
        await bot.send_message(chat_id, 'Я не смог определить, к какой социальной сети относятся ссылки\n'
                                        'Обратитесь к @Vertonger')
        parsing = False
        for user in waiting_users:
            await bot.send_message(user, 'Я закончил и готов выполнить следующий запрос')
        waiting_users = set()
        return

    if len(all_posts_data) == 0:
        await bot.send_message(chat_id, 'В заданном диапазоне времени нет постов')
    else:
        all_posts_data = {'Данные': all_posts_data}
        file_name = f"{period_begin_str.replace('.', '_')}-{period_end_str.replace('.', '_')}.xls"
        pyexcel.save_book_as(bookdict=all_posts_data, dest_file_name=file_name)
        await bot.edit_message_text(chat_id=chat_id, message_id=curr_mess.message_id,
                                    text='Загружаю файл, это может занять некоторое время', parse_mode='HTML')

        with open(file_name, 'rb') as file:
            await bot.send_document(chat_id, file, caption='')

        if os.path.getsize('error_links.txt') > 39:
            with open('error_links.txt', 'rb') as file:
                await bot.send_document(chat_id, file, caption=f'Некорректные ссылки')
        await bot.edit_message_text(chat_id=chat_id,
                                    message_id=curr_mess.message_id,
                                    text=f'Всего обработано постов: {total_post_cnt}')

    logger.info(f'Обработано {total_post_cnt} постов за {time.time() - start_time} секунд')
    parsing = False
    for user in waiting_users:
        await bot.send_message(user, 'Я закончил и готов выполнить следующий запрос')
    waiting_users = set()


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
