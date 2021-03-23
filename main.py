import sys
import time
from datetime import datetime
from http.client import IncompleteRead

from dateutil import relativedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

DEBUG_MODE = False
MESSAGE_LIMIT = 1000

EXCLUDE_CHANNEL_PATTERN = ('log-', 'log_')

# ref. https://api.slack.com/docs/rate-limits
SLEEP_BUFFER = 0.1
SLEEP_TIER1 = 60 / 1 + SLEEP_BUFFER
SLEEP_TIER2 = 60 / 20 + SLEEP_BUFFER
SLEEP_TIER3 = 60 / 50 + SLEEP_BUFFER
SLEEP_TIER4 = 60 / 100 + SLEEP_BUFFER

RETRY = 3

# survey priod
LATEST = datetime.now()
OLDEST = LATEST - relativedelta.relativedelta(years=1)
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'

public_channel_map = None
custom_emoji_names = None


def get_custom_emoji_names():
    DEBUG_MODE and print('DEBUG: start getting custom emoji names...')
    if custom_emoji_names is not None:
        DEBUG_MODE and print('DEBUG: already have custom emoji.')
        return custom_emoji_names

    for _ in range(RETRY):
        try:
            response = client.emoji_list()
        except SlackApiError as e:
            print('Error: ', e.response['error'])
        except IncompleteRead as e:
            print('IncompleteRead Exception: ', e)
        else:
            break
    else:
        print('Failed to get custom emoji list.')
        return None

    DEBUG_MODE and print('DEBUG: end getting custom emoji names.')
    return list(response['emoji'].keys())


def get_public_channel_map():
    DEBUG_MODE and print('DEBUG: start getting public channel map...')
    if public_channel_map is not None:
        DEBUG_MODE and print('DEBUG: already have public channel map.')
        return public_channel_map

    channel_ids = {}
    for _ in range(RETRY):
        try:
            cursor = None
            while True:
                response = client.conversations_list(
                    exclude_archived=True, limit=1000, cursor=cursor)
                channel_ids.update(
                    {channel['name']: channel['id'] for channel in response['channels']})

                has_more = response['has_more']
                if has_more:
                    cursor = response['response_metadata']['next_cursor']
                    time.sleep(SLEEP_TIER2)
                else:
                    break
        except SlackApiError as e:
            print('Error: ', e.response['error'])
        except IncompleteRead as e:
            print('IncompleteRead Exception: ', e)
        else:
            break
    else:
        print('Failed to get public channel list.')
        return None

    DEBUG_MODE and print('DEBUG: end getting public channel map.')
    return channel_ids


def get_public_channel_id_by_name(channel_name):
    channel_map = get_public_channel_map()
    return channel_map.get(channel_name, '') if channel_map else None


def get_emoji_count(channel_name):
    DEBUG_MODE and print('DEBUG: start counting in {}...'.format(channel_name))

    result = {}
    messages = get_messages(channel_name)
    if messages is None:
        print(f'Failed to get emoji count in {channel_name}')
        return None

    for message in messages:
        # count in text
        for block in message.get('blocks', []):
            for elem in block.get('elements', []):
                for e in elem.get('elements', []):
                    if e['type'] == 'emoji':
                        emoji_name = e['name']
                        total = result.get(emoji_name, 0) + 1
                        result.update({emoji_name: total})

        # count in reactions
        if 'reactions' not in message:
            continue
        for reaction in message['reactions']:
            emoji_name = reaction['name']
            emoji_count = reaction['count']
            total = emoji_count + result.get(emoji_name, 0)
            result.update({emoji_name: total})

    DEBUG_MODE and print('DEBUG: end counting.')
    return result


def get_emoji_count_in_all_public_channel():
    channel_map = get_public_channel_map()

    result = {}
    channel_names = [
        name for name in channel_map.keys() if not name.startswith(EXCLUDE_CHANNEL_PATTERN)
    ]
    for index, channel_name in enumerate(channel_names):
        print('surveying in {} ({}/{})...'.format(channel_name,
              index + 1, len(channel_names)))
        sub_result = get_emoji_count(channel_name)
        if sub_result is None:
            print('Failed to get emoji count in all channel.')
            return None

        for emoji_name, count in sub_result.items():
            result.update({emoji_name: count + result.get(emoji_name, 0)})

    return result


def get_custom_emoji_count(channel_name):
    DEBUG_MODE and print('DEBUG: start counting in {}...'.format(channel_name))

    custom_emoji_names = get_custom_emoji_names()

    result = {}
    messages = get_messages(channel_name)
    if messages is None:
        print(f'Failed to get emoji count in {channel_name}')
        return None

    for message in messages:
        # count in text
        for block in message.get('blocks', []):
            for elem in block.get('elements', []):
                for e in elem.get('elements', []):
                    if e['type'] == 'emoji':
                        emoji_name = e['name']
                        if emoji_name in custom_emoji_names:
                            total = result.get(emoji_name, 0) + 1
                            result.update({emoji_name: total})

        # count in reactions
        if 'reactions' not in message:
            continue
        for reaction in message['reactions']:
            emoji_name = reaction['name']
            emoji_count = reaction['count']
            if emoji_name in custom_emoji_names:
                total = emoji_count + result.get(emoji_name, 0)
                result.update({emoji_name: total})

    DEBUG_MODE and print('DEBUG: end counting.')
    return result


def get_custom_emoji_count_in_all_public_channel():
    channel_map = get_public_channel_map()

    result = {}
    channel_names = [
        name for name in channel_map.keys() if not name.startswith(EXCLUDE_CHANNEL_PATTERN)
    ]
    for index, channel_name in enumerate(channel_names):
        print('surveying in {} ({}/{})...'.format(channel_name,
              index + 1, len(channel_names)))
        sub_result = get_custom_emoji_count(channel_name)
        if sub_result is None:
            print('Failed to get custom emoji count in all channel.')
            return None

        for emoji_name, count in sub_result.items():
            result.update({emoji_name: count + result.get(emoji_name, 0)})

    return result


def get_messages(channel_name, contains_reply=True):
    DEBUG_MODE and print(
        'DEBUG: start getting messages in {}...'.format(channel_name))

    channel_id = get_public_channel_id_by_name(channel_name)

    DEBUG_MODE and print('DEBUG: channel_id is {}'.format(channel_id))

    result = []
    for _ in range(RETRY):
        try:
            cursor = None
            while True:
                response = client.conversations_history(
                    channel=channel_id, limit=MESSAGE_LIMIT, cursor=cursor, latest=LATEST.timestamp(), oldest=OLDEST.timestamp())
                print(
                    ' -> {} messages fetched.'.format(len(response['messages'])))
                DEBUG_MODE and print('DEBUG: {}'.format(response))

                messages = response['messages']
                result.extend(messages)

                if contains_reply:
                    for message in messages:
                        if 'reply_count' not in message:
                            continue

                        thread_ts = message['thread_ts']
                        replies = get_replies(channel_name, thread_ts)
                        if replies is None:
                            raise SlackApiError(
                                f'Failed to get replies. {channel_name}, {thread_ts}')
                        result.extend(replies)

                has_more = response['has_more']
                if has_more:
                    cursor = response['response_metadata']['next_cursor']
                    time.sleep(SLEEP_TIER3)
                else:
                    break

            time.sleep(SLEEP_TIER3)
        except SlackApiError as e:
            print('Error: ', e.response['error'])
        except IncompleteRead as e:
            print('IncompleteRead Exception: ', e)
        else:
            break
    else:
        print('Failed to get messages in {}.'.format(channel_name))
        return None

    DEBUG_MODE and print('DEBUG: end getting messages.')
    return result


def get_replies(channel_name, thread_ts):
    DEBUG_MODE and print('DEBUG: start getting replies...')

    channel_id = get_public_channel_id_by_name(channel_name)

    result = []
    for _ in range(RETRY):
        try:
            cursor = None
            while True:
                response = client.conversations_replies(
                    channel=channel_id, ts=thread_ts, limit=MESSAGE_LIMIT, cursor=cursor, latest=LATEST.timestamp(), oldest=OLDEST.timestamp())
                print(
                    ' ---> {} replies fetched.'.format(len(response['messages'])))
                DEBUG_MODE and print('DEBUG: {}'.format(response))

                replies = [message for message in response['messages']
                           if message['ts'] != thread_ts]
                result.extend(replies)

                has_more = response['has_more']
                if has_more:
                    cursor = response['response_metadata']['next_cursor']
                    time.sleep(SLEEP_TIER3)
                else:
                    break

            time.sleep(SLEEP_TIER3)
        except SlackApiError as e:
            print('Error: ', e.response['error'])
        except IncompleteRead as e:
            print('IncompleteRead Exception: ', e)
        else:
            break
    else:
        print('Failed to get replies in {}.'.format(channel_name))
        return None

    DEBUG_MODE and print('DEBUG: end getting replies.')
    return result


def post_message(client, channel_name, message):
    channel = channel_name if channel_name.startswith(
        '#') else '#{}'.format(channel_name)

    if DEBUG_MODE:
        print('DEBUG: following message was posted to {}:\n {}'.format(
            channel_name, message))
        return 'debug mode'

    for _ in range(RETRY):
        try:
            response = client.chat_postMessage(channel=channel, text=message)
        except SlackApiError as e:
            print('Error: ', e.response['error'])
        except IncompleteRead as e:
            print('IncompleteRead Exception: ', e)
        else:
            break
    else:
        print('Failed to post message to {}: {}'.format(channel, message))
        return 'failed'

    print('succeeded to post following message to {}\n{}\n--------\n\n'.format(
        channel_name, message[:1000]))
    return 'succeeded'


def get_top_emoji_count(emoji_count, limit=10):
    sorted_count = sorted(emoji_count.items(),
                          key=lambda x: x[1], reverse=True)[:limit]
    DEBUG_MODE and print(sorted_count)

    return sorted_count


def get_post_message_by_sorted_count(sorted_count):
    message = ''

    for emoji_name, count in sorted_count:
        message += '> :{}: : {}回\n'.format(emoji_name, count)

    DEBUG_MODE and print('DEBUG: return message: {}'.format(message))
    return message


def get_unused_custom_emojis(emoji_count, limit=3):
    result = {}

    custom_emoji_names = get_custom_emoji_names()
    all_custom_emoji_count = {custom_emoji_name: emoji_count.get(
        custom_emoji_name, 0) for custom_emoji_name in custom_emoji_names}
    for i in range(limit + 1):
        result[i] = [custom_emoji_name for custom_emoji_name,
                     count in all_custom_emoji_count.items() if count == i]

    DEBUG_MODE and print(result)
    return result


def get_post_message_by_unused_custom_emojis(unused_custom_emojis):
    message = ''

    for count, emoji_names in unused_custom_emojis.items():
        message += '*{}回*/n/n :{}:\n'.format(count, ': :'.join(emoji_names))

    DEBUG_MODE and print('DEBUG: return message: {}'.format(message))
    return message


# main
# inputs
token = input('User OAuth Token: ')
client = WebClient(token=token)
bot_token = input('Bot User OAuth Token: ')
bot_client = WebClient(token=bot_token)

public_channel_map = get_public_channel_map()
if public_channel_map is None:
    sys.exit(1)

while True:
    ranking_type = input('Choose ranking type [top/unused]: ')
    if ranking_type not in ['top', 'unused']:
        print('Invalid ranking type. Try again.')
        continue
    if ranking_type == 'top':
        ranking_limit = input('Choose ranking limit (default: 10): ')
    else:
        ranking_limit = input('Choose ranking limit (default: 0): ')
    if ranking_limit != '' and not ranking_limit.isnumeric():
        print('Invalid ranking limit. Need to input number. Try again.')
        continue
    else:
        default_limit = 10 if ranking_type == 'top' else 0
        ranking_limit = int(ranking_limit or default_limit)

    break

while True:
    if ranking_type == 'top':
        target_channel = input(
            'Channel name to survey (default: all channel): ')
    else:
        confirm_reccomended = input(
            'Surveying all channel is reccomended for unused ranking. Are you sure? [Y/n]: ')
        if confirm_reccomended == 'n':
            target_channel = input(
                'Channel name to survey (default: all channel): ')
        else:
            target_channel = ''

    if ranking_type == 'top' and target_channel == '':
        confirm = input(
            'It takes long time to survey all channels. Continue? [y/N]: ')
        if confirm != 'y':
            continue
        else:
            print('continue.')
    if target_channel != '' and target_channel not in public_channel_map.keys():
        print('Error: invalid channel name. Try again.')
        continue
    break

if ranking_type == 'top':
    while True:
        post_channel_name = input('Channel name to post message: ')
        if post_channel_name not in public_channel_map.keys():
            print('Error: invalid channel name. Try again.')
            continue
        break
    while True:
        emoji_type = input('Choose emoji type [custom/all] (default: all): ')
        if emoji_type not in ['', 'custom', 'all']:
            print('Error: invalid emoji type. Choose [custom/all]. Try again.')
            continue
        break
else:
    # Don't post to slack cause unused ranking message is too log to post
    post_channel_name = ''
    # Only custom is available for unused ranking
    emoji_type = 'custom'

print('\nstart surveying...\n')

if target_channel == '':
    result = get_custom_emoji_count_in_all_public_channel(
    ) if emoji_type == 'custom' else get_emoji_count_in_all_public_channel()
else:
    result = get_custom_emoji_count(
        target_channel) if emoji_type == 'custom' else get_emoji_count(target_channel)

if result is None:
    print('Failed to get count result.')
    sys.exit(1)

# message header
message_header = ''
if ranking_type == 'top':
    message_header += '*Emojiランキング* Top {}\n\n'.format(ranking_limit)
else:
    message_header += '*使っていないEmojiランキング* Under {}\n\n'.format(ranking_limit)
message_header += '集計範囲： '
if target_channel == '':
    message_header += 'すべてのパブリックチャンネル（log系チャンネルを除く）\n'
else:
    message_header += '<#{}>\n'.format(public_channel_map[target_channel])
message_header += '集計対象： '
message_header += 'カスタムEmojiのみ\n' if emoji_type == 'custom' or ranking_type == 'unused' else 'すべてのEmoji\n'
message_header += '集計期間： {} ~ {}\n'.format(
    OLDEST.strftime(DATE_FORMAT), LATEST.strftime(DATE_FORMAT))
message_header += '\n'

# sort and create message
if ranking_type == 'top':
    sorted_result = get_top_emoji_count(result, ranking_limit)
    message = message_header + get_post_message_by_sorted_count(sorted_result)
    post_message(bot_client, post_channel_name, message)

    # post deletable message
    # post_message(client, post_channel_name, message)
else:
    unused_custom_emojis = get_unused_custom_emojis(result, ranking_limit)
    message = message_header + \
        get_post_message_by_unused_custom_emojis(unused_custom_emojis)
    # Don't post to slack cause unused ranking message is too log to post
    print('--------\n\n{}\n\n--------'.format(message))

print('\nend surveying.\n')
