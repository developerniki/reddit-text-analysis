import json
from pathlib import Path
from statistics import mean

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DATA_PATH = next((Path(__file__).parent / 'archives').iterdir())
KEYWORD = ' Tate'


def sentences_around_keyword(text: str, keyword: str, n: int, case_sensitive: bool = False) -> str:
    """Return the first `n` sentences around the first occurrence of `keyword` in `text`."""
    sentences = text.split('.')
    for i, sentence in enumerate(sentences):
        if keyword.casefold() in sentence.casefold() if not case_sensitive else keyword in sentence:
            start = max(0, i - n)
            end = min(len(sentences), i + n + 1)
            return '.'.join(sentences[start:end])
    return ''


def main() -> None:
    # Load the dataset and print some information about the dataset.
    data = json.loads(DATA_PATH.read_text())
    print(f'The first submission this dataset was posted on {data[-1]["created_utc"]} UTC. The last one was posted on '
          f'{data[0]["created_utc"]} UTC.')
    print()

    # Flatten the list of submissions into a list of submissions and a list of comments/replies and sort them by UTC
    # timestamp.
    stack = data.copy()
    del data
    submissions = []
    comments_and_replies = []
    while stack:
        s = stack.pop()
        if s.get('comments'):
            stack.extend(s['comments'])
            del s['comments']
        elif s.get('replies'):
            stack.extend(s['replies'])
            del s['replies']
        if 'selftext' in s:
            submissions.append(s)
        elif 'body' in s:
            comments_and_replies.append(s)
        else:
            raise ValueError(f'Unknown item: {s}')
    submissions.sort(key=lambda d: d['created_utc'])
    comments_and_replies.sort(key=lambda d: d['created_utc'])

    # Filter out all items that were removed by moderators, deleted by the author or posted by AutoModerator.
    print(f'{len(submissions)} submissions and {len(comments_and_replies)} comments/replies before filtering out '
          f'removed/deleted items and AutoModerator...')
    submissions = [d for d in submissions
                   if d['removed_by_category'] is None
                   and d['author_name'] != 'AutoModerator']
    comments_and_replies = [d for d in comments_and_replies
                            if d['body'] not in ['[removed]', '[deleted]'] and d['author_name'] != 'AutoModerator']
    print(f'{len(submissions)} submissions and {len(comments_and_replies)} comments/replies remain.')
    print()

    # Retain only items that contain the keyword.
    print(f'Next, we filter out all items that do not contain the text "{KEYWORD}"...')
    submissions = [d for d in submissions if KEYWORD.casefold() in d['selftext'].casefold()
                   or KEYWORD.casefold() in d['title'].casefold()]
    comments_and_replies = [d for d in comments_and_replies if KEYWORD.casefold() in d['body'].casefold()]
    print(f'{len(submissions)} submissions and {len(comments_and_replies)} comments/replies remain.')
    print()

    # Retain only items that have a score of at least 1.
    print('Next, we only retain items that have a score of at least 1...')
    submissions = [d for d in submissions if d['score'] > 1]
    comments_and_replies = [d for d in comments_and_replies if d['score'] > 1]
    print(f'{len(submissions)} submissions and {len(comments_and_replies)} comments/replies remain.')
    print()

    # Sort items by score (descending).
    submissions.sort(key=lambda d: d['score'], reverse=True)
    comments_and_replies.sort(key=lambda d: d['score'], reverse=True)

    # Do a sentiment analysis on the submissions and comments/replies.
    analyzer = SentimentIntensityAnalyzer()
    sentiments = []

    # Print scored titles + submission texts with the keyword (1 sentence before and after).
    print(120 * '=')
    print(f'Scored titles + submission texts with "{KEYWORD}" (1 sentence before and after).')
    print(120 * '=')
    for s in submissions:
        if KEYWORD.casefold() in s['selftext'].casefold():
            url = f'https://www.reddit.com{s["permalink"]}'
            surrounding_text = sentences_around_keyword(s["title"] + '\n' + s['selftext'], KEYWORD, n=1)
            print(f'{s["score"]}: {surrounding_text} | url: {url}')
            sentiment = analyzer.polarity_scores(surrounding_text)
            sentiments.append(sentiment)
            print(f'sentiment: {sentiment}')
            print(120 * '-')
            print()

    # Print scored comments/replies with the keyword (1 sentence before and after).
    print(120 * '=')
    print(f'Scored comments/replies with "{KEYWORD}" (1 sentence before and after).')
    print(120 * '=')
    for c in comments_and_replies:
        if KEYWORD.casefold() in c['body'].casefold():
            url = f'https://www.reddit.com{c["permalink"]}'
            surrounding_text = sentences_around_keyword(c['body'], KEYWORD, n=1)
            print(f'{c["score"]}: {surrounding_text} | url: {url}')
            sentiment = analyzer.polarity_scores(surrounding_text)
            sentiments.append(sentiment)
            print(f'sentiment: {sentiment}')
            print(120 * '-')
            print()

    # Summarize the sentiments by calculating the mean of each sentiment score.
    sentiment_neg = mean([s['neg'] for s in sentiments])
    sentiment_neu = mean([s['neu'] for s in sentiments])
    sentiment_pos = mean([s['pos'] for s in sentiments])
    sentiment_compound = mean([s['compound'] for s in sentiments])
    print(f'Sentiment summary: neg={sentiment_neg}, neu={sentiment_neu}, pos={sentiment_pos}, '
          f'compound={sentiment_compound}')

    # Get number of more positive and more negative sentiments.
    num_more_positive = len([s for s in sentiments if s['neg'] < s['pos']])
    num_more_negative = len([s for s in sentiments if s['neg'] > s['pos']])
    print(f'Number of more positive sentiments: {num_more_positive}')
    print(f'Number of more negative sentiments: {num_more_negative}')


if __name__ == '__main__':
    main()
