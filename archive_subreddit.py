#!/usr/bin/env python3

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

import reddit_utils
from reddit_utils import Pushshift

ARCHIVES_DIR = Path(__file__).parent / 'archives'
ARCHIVES_DIR.mkdir(exist_ok=True)
SAVE_EVERY_N_SUBMISSIONS = 100


def prompt_str_until_valid(prompt: str) -> str:
    """Repeatedly ask for input until a valid string is entered."""
    while True:
        value = input(prompt)
        if value == '':
            print('Please enter a non-empty string.', end='\n\n')
            continue
        return value


def prompt_optional_int_until_valid(prompt: str) -> Optional[int]:
    """Repeatedly ask for input until a valid integer or empty string is entered."""
    while True:
        value = input(prompt)
        if value == '':
            return None
        try:
            return int(value)
        except ValueError:
            print('Please enter a valid integer or leave empty.', end='\n\n')


def prompt_archive_choice(pattern: re.Pattern) -> Optional[Path]:
    """Prompt the user to choose an archive to continue from.

    Args:
        pattern: The pattern to match the archive files against. Has to contain the named groups 'max_submissions',
            't_start' and 't_end'.

    Returns:
        The path to the archive to continue from, or None if the user wants to start from scratch.
    """
    existing_archives = [file for file in ARCHIVES_DIR.iterdir() if pattern.match(file.name)]
    if not existing_archives:
        return None

    print('The following intermediate files were found:')
    for i, file in enumerate(existing_archives):
        match = pattern.match(file.name)
        max_submissions = match.group('max_submissions')
        max_submissions = 'all' if max_submissions is None else int(max_submissions)
        t_start = datetime.fromtimestamp(float(match.group('t_start')) / 1000.0)
        t_end = datetime.fromtimestamp(float(match.group('t_end')) / 1000.0)
        print(f'{i + 1}. {file.name} '
              f'(submissions to retrieve: {max_submissions}, UTC date time range: {t_start} to {t_end})')

    while True:
        choice = input('Which file to continue from? Enter nothing to start from scratch: ')
        if choice == '':
            return None
        elif not choice.isdigit() or not 1 <= int(choice) <= len(existing_archives):
            print('Please enter a valid integer from the list above.')
        else:
            return existing_archives[int(choice) - 1]


def main() -> None:
    # Prompt the user for the necessary information and load the intermediate archive if it exists.
    subreddit = re.match(r'(r/)?(.+)', prompt_str_until_valid('Enter the subreddit name: ')).group(2).lower()
    pattern = re.compile(fr'intermediate_{subreddit}_(?P<max_submissions>\d+|inf)_(?P<t_start>\d+)_(?P<t_end>\d+).json')
    archive_file = prompt_archive_choice(pattern)
    if archive_file is not None:
        match = pattern.match(archive_file.name)
        max_submissions = match.group('max_submissions')
        max_submissions = None if max_submissions == 'inf' else int(max_submissions)
        t_start_ms = int(match.group('t_start'))
        t_end_ms = int(match.group('t_end'))
        posts = json.loads(archive_file.read_text())
        before = posts[-1]['created_utc']
        print(f'Continuing from "{archive_file.name}"...')
    else:
        max_submissions = prompt_optional_int_until_valid('Enter the maximum number of submissions to retrieve: ')
        t_start_ms = round(time.time_ns() / 1_000_000)
        t_end_ms = t_start_ms
        before = None
        posts = []
        print(f'Starting from scratch ({max_submissions} submissions)...')

    # Retrieve the submissions and comments.
    pushshift = Pushshift()  # Use PushShift API to discover submissions (official API has limit of 1,000 most recent).
    reddit = reddit_utils.init_reddit()  # Use official Reddit API to fetch submissions and comments.

    # total_submission_count = pushshift.query_submission_count(subreddit)
    # if max_submissions is not None:
    #     total_submission_count = min(total_submission_count, max_submissions)

    # Unfortunately, the above code does not work. The Pushshift API does not return the correct number of submissions
    # as it used to.
    total_submission_count = max_submissions

    with tqdm(initial=len(posts), total=total_submission_count, desc='submissions') as pbar:
        for submission in pushshift.query_submissions(subreddit, count=max_submissions - len(posts), before=before):
            submission = reddit.submission(submission['id'])
            comments = reddit_utils.fetch_comments_for_submission(submission)
            submission = reddit_utils.submission_to_dict(submission)
            submission['comments'] = [reddit_utils.comment_to_dict(comment) for comment in comments]
            submission = dict(sorted(submission.items()))
            posts.append(submission)
            t_end_ms = round(time.time_ns() / 1_000_000)
            pbar.update(1)

            # Save the intermediate archive every `SAVE_EVERY_N_SUBMISSIONS` submissions, deleting the previous one.
            if len(posts) % SAVE_EVERY_N_SUBMISSIONS == 0:
                old_archive_file = archive_file
                archive_file = ARCHIVES_DIR / f'intermediate_{subreddit}_{max_submissions}_{t_start_ms}_{t_end_ms}.json'
                print(f'Saving intermediate archive to "{archive_file.name}"...')
                archive_file.write_text(json.dumps(posts, indent=2))
                if old_archive_file is not None:
                    old_archive_file.unlink()

    # Save the final archive, deleting the previous one.
    old_archive_file = archive_file
    archive_file = ARCHIVES_DIR / f'{subreddit}_{len(posts)}_{t_start_ms}_{t_end_ms}.json'
    print(f'Saving final archive to "{archive_file.name}"...')
    archive_file.write_text(json.dumps(posts, indent=2))
    if old_archive_file is not None:
        old_archive_file.unlink()


if __name__ == '__main__':
    main()
