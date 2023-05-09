"""This module provides utility functions for querying submissions and comments from Reddit."""

import json
import time
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Optional, Generator, Dict, Any

from praw import Reddit
from praw.models import Submission, Comment
from praw.models.comment_forest import CommentForest
from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry
from requests_toolbelt.sessions import BaseUrlSession

PRAW_CREDENTIALS_FILE = Path(__file__).parent / 'credentials' / 'praw_credentials.json'


class PushshiftError(Exception):
    """The base class for all errors raised by the Pushshift API."""


class PushshiftHTTPError(PushshiftError):
    """An error raised by the Pushshift API due to an HTTP error."""

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


class PushshiftJSONError(PushshiftError):
    """An error raised by the Pushshift API due to an invalid JSON response."""

    def __init__(self, message: str, response_text: str):
        super().__init__(message)
        self.response_text = response_text


class PushshiftKeyError(PushshiftError):
    """An error raised by the Pushshift API due to a missing key in the response."""

    def __init__(self, message: str, response_json: dict):
        super().__init__(message)
        self.response_json = response_json


class Pushshift(BaseUrlSession):
    """This class wraps the PushShift API. It handles the ratelimit and provides methods to query submissions and
    comments from subreddits."""

    def __init__(self):
        super().__init__(base_url='https://api.pushshift.io/')
        server_ratelimit_per_minute = 60
        self._wait_between_secs = 60.0 / server_ratelimit_per_minute
        self._last_request_time = 0
        self._request_size = 500
        retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504, 524])
        self.mount('https://', HTTPAdapter(max_retries=retries))

    def query_submission_count(self, subreddit: str) -> int:
        """Query the number of submissions in a subreddit.

        Args:
            subreddit: The name of the subreddit to query.

        Returns:
            The number of submissions in the subreddit.

        Raises:
            PushshiftHTTPError: If the request fails.
            PushshiftJSONError: If the response is not valid JSON.
            PushshiftKeyError: If the response does not contain the expected keys.
        """
        self._handle_ratelimit_before_request()
        try:
            resp = self.get('reddit/search/submission', params={'subreddit': subreddit, 'metadata': True})
            resp.raise_for_status()
        except HTTPError as e:
            raise PushshiftHTTPError(str(e), e.response.status_code)
        try:
            resp = resp.json()
        except JSONDecodeError as e:
            raise PushshiftJSONError(str(e), resp.text)
        try:
            return resp['metadata']['total_results']
        except KeyError as e:
            raise PushshiftKeyError(str(e), resp)

    def query_submissions(
            self,
            subreddit: str,
            count: Optional[int] = None,
            before: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Query submissions from a subreddit. Yields submissions one by one.

        Args:
            subreddit: The name of the subreddit to query.
            count: The maximum number of submissions to yield. If None, all submissions will be yielded.
            before: The UTC timestamp in seconds to query before. If None, queries from the most recent submissions.

        Yields:
            The submissions as dicts.

        Raises:
            PushshiftHTTPError: If the request fails.
            PushshiftJSONError: If the response is not valid JSON.
            PushshiftKeyError: If the response does not contain the expected keys.
        """
        return self._query_items(item_type='submission', subreddit_or_id=subreddit, count=count, before=before)

    def query_comments(
            self,
            submission_id: str,
            count: Optional[int] = None,
            before: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Query comments from a submission. Yields comments one by one.

        Args:
            submission_id: The ID of the submission to query.
            count: The maximum number of comments to yield. If None, all comments will be yielded.
            before: The UTC timestamp in seconds to query before. If None, queries from the most recent submissions.

        Yields:
            The comments as dicts.

        Raises:
            PushshiftHTTPError: If the request fails.
            PushshiftJSONError: If the response is not valid JSON.
            PushshiftKeyError: If the response does not contain the expected keys.
        """
        return self._query_items(item_type='comment', subreddit_or_id=submission_id, count=count, before=before)

    def _query_items(
            self,
            item_type: str,
            subreddit_or_id: str,
            count: Optional[int] = None,
            before: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Query items from a subreddit or submission/comment ID. Yields items one by one. Supported item types are
        'submission' and 'comment'."""
        if item_type not in ('submission', 'comment'):
            raise NotImplementedError("Only item type 'submission' or 'comment' is allowed.")

        pending_items = float('inf') if count is None else count
        while pending_items > 0:
            self._handle_ratelimit_before_request()
            id_key = 'subreddit' if item_type == 'submission' else 'link_id'
            params = {id_key: subreddit_or_id, 'size': self._request_size, 'before': before}
            try:
                resp = self.get(f'reddit/search/{item_type}', params=params)
                resp.raise_for_status()
            except HTTPError as e:
                raise PushshiftHTTPError(str(e), e.response.status_code)
            try:
                resp = resp.json()
            except JSONDecodeError as e:
                raise PushshiftJSONError(str(e), resp.text)
            try:
                items = resp['data']
            except KeyError as e:
                raise PushshiftKeyError(str(e), resp)

            if not items:
                break

            for item in items[:None if pending_items == float('inf') else pending_items]:
                pending_items -= 1
                before = item['created_utc']
                yield item

    def _handle_ratelimit_before_request(self):
        """Handle the ratelimit before a request by waiting if necessary."""
        time.sleep(max(0.0, self._wait_between_secs - (time.perf_counter() - self._last_request_time)))
        self._last_request_time = time.perf_counter()


def submission_to_dict(submission: Submission, datetime_fmt='%Y-%m-%d %H:%M:%S') -> Dict[str, Any]:
    """Parse a reddit submission object and write the relevant attributes into a dictionary.

    Args:
        submission: A praw.reddit.Submission instance.
        datetime_fmt: The format to use when parsing the submission's creation date.

    Returns:
        A dictionary containing the parsed submission.
    """
    submission_parsed = {
        'author_name': submission.author and submission.author.name,
        'author_flair_text': submission.author_flair_text,
        'comments': None,
        'created_utc': datetime.fromtimestamp(submission.created_utc).strftime(datetime_fmt),
        'distinguished': submission.distinguished,
        'edited': submission.edited,
        'id': submission.id,
        'is_original_content': submission.is_original_content,
        'link_flair_text': submission.link_flair_text,
        'locked': submission.locked,
        'name': submission.name,
        'num_comments': submission.num_comments,  # Includes deleted, removed, and spam comments.
        'over_18': submission.over_18,
        'permalink': submission.permalink,
        'removed_by_category': submission.removed_by_category,
        'score': submission.score,
        'selftext': submission.selftext,
        'spoiler': submission.spoiler,
        'stickied': submission.stickied,
        'subreddit_display_name': submission.subreddit.display_name,
        'title': submission.title,
        'upvote_ratio': submission.upvote_ratio,
        'url': submission.url,
    }
    return submission_parsed


def init_reddit() -> Reddit:
    """Initialize the Reddit API wrapper. Reads the PRAW credentials from a JSON file which must be located at
    `credentials/praw_credentials.json` relative to this file."""
    praw_credentials = json.loads(PRAW_CREDENTIALS_FILE.read_text())
    reddit = Reddit(**praw_credentials)
    reddit.read_only = True
    return reddit


def fetch_comments_for_submission(submission: Submission, limit: Optional[int] = None) -> CommentForest:
    """Fetch the comments for a submission.

    Args:
        submission: A praw.reddit.Submission instance.
        limit: The maximum number of comments to fetch. If None, all comments will be fetched.

    Returns:
        A praw.models.comment_forest.CommentForest instance.
    """
    comments = submission.comments
    comments.replace_more(limit=limit)
    return comments


def comment_to_dict(comment: Comment, datetime_fmt='%Y-%m-%d %H:%M:%S') -> Dict[str, Any]:
    """Parse a reddit comment object and write the relevant attributes into a dictionary.

    Args:
        comment: A praw.reddit.Comment instance.
        datetime_fmt: The format to use when parsing the comment's creation date.

    Returns:
        A dictionary containing the parsed comment.
    """
    comment_parsed = {
        # The author is None if the Reddit account does not exist anymore.
        'author_name': comment.author and comment.author.name,
        'body': comment.body,
        'created_utc': datetime.fromtimestamp(comment.created_utc).strftime(datetime_fmt),
        'distinguished': comment.distinguished,
        'edited': comment.edited,
        'id': comment.id,
        'is_submitter': comment.is_submitter,
        'link_id': comment.link_id,
        'parent_id': comment.parent_id,
        'permalink': comment.permalink,
        'replies': [comment_to_dict(comment) for comment in comment.replies],
        'score': comment.score,
        'stickied': comment.stickied,
    }
    return comment_parsed


def is_submission_created_in_last_n_hours(
        submission: Submission | Dict[str, Any],
        hours: int,
        datetime_fmt='%Y-%m-%d %H:%M:%S'
) -> bool:
    """Check if the submission was created in the last `hours` hours.

    Args:
        submission: A `praw.reddit.Submission` instance or a dictionary containing the parsed submission.
        hours: The number of hours.
        datetime_fmt: The format to use when parsing the submission's creation date.

    Returns:
        True if the submission was created in the last `hours` hours, False otherwise.
    """
    submission_created = submission['created_utc'] if isinstance(submission, dict) else submission.created_utc
    submission_created = datetime.strptime(submission_created, datetime_fmt)
    now = datetime.now()
    return (now - submission_created).total_seconds() < hours * 3600
