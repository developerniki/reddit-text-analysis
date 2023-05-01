import time
from json import JSONDecodeError
from typing import Optional, Generator, Dict, Any

from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry
from requests_toolbelt.sessions import BaseUrlSession


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
