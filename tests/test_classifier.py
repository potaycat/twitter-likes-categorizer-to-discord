import unittest
from types import SimpleNamespace

import httpx

from app import (
    ClassificationResult,
    ExtractedTweet,
    TweetFetchError,
    TweetClassifier,
    fetch_tweet_from_official_api,
)


class FakeDecisions:
    def __init__(self, answers: dict[str, dict[str, object]]) -> None:
        self.answers = answers

    def create(self, **kwargs: object) -> dict[str, object]:
        return {"answers": self.answers}


class TweetClassifierConfidenceTests(unittest.IsolatedAsyncioTestCase):
    async def classify_answers(
        self,
        answers: dict[str, dict[str, object]],
        second_threshold: float = 0.39,
    ) -> ClassificationResult:
        classifier = object.__new__(TweetClassifier)
        classifier.client = SimpleNamespace(decisions=FakeDecisions(answers))
        classifier.confidence_threshold = 0.55
        classifier.second_category_confidence_threshold = second_threshold
        classifier.image_resolution = "medium"
        tweet = ExtractedTweet(
            tweet_id="test",
            text="",
            user_name="",
            user_screen_name="",
            user_profile_image_url="",
            tweet_url="",
            date_epoch=None,
            likes=0,
            retweets=0,
            replies=0,
            hashtags=[],
            media_urls=[],
            media_extended=[],
        )
        return await classifier.classify(tweet, [], False)

    async def test_low_confidence_primary_category_falls_back_to_other(self) -> None:
        result = await self.classify_answers(
            {"post_category": {"choice": "meme", "confidence": 0.35}}
        )

        self.assertEqual(result.category, "other")
        self.assertEqual(result.confidence, 0.35)

    async def test_second_category_below_its_threshold_falls_back_to_other(self) -> None:
        result = await self.classify_answers(
            {
                "post_category": {"choice": "other", "confidence": 0.2},
                "second_post_category": {"choice": "meme", "confidence": 0.35},
            }
        )

        self.assertEqual(result.category, "other")
        self.assertEqual(result.confidence, 0.2)

    async def test_second_category_at_its_threshold_is_accepted(self) -> None:
        result = await self.classify_answers(
            {
                "post_category": {"choice": "other", "confidence": 0.2},
                "second_post_category": {"choice": "meme", "confidence": 0.39},
            }
        )

        self.assertEqual(result.category, "meme")
        self.assertEqual(result.confidence, 0.39)


class OfficialXApiFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_and_maps_official_api_response(self) -> None:
        request_count = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            self.assertEqual(request.headers["Authorization"], "Bearer test-bearer")
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "123",
                        "text": "Official tweet",
                        "author_id": "42",
                        "created_at": "2024-01-02T03:04:05.000Z",
                        "public_metrics": {
                            "like_count": 7,
                            "retweet_count": 3,
                            "reply_count": 2,
                        },
                        "entities": {"hashtags": [{"tag": "furry"}]},
                        "attachments": {"media_keys": ["3_1"]},
                    },
                    "includes": {
                        "users": [
                            {
                                "id": "42",
                                "name": "Test User",
                                "username": "testuser",
                                "profile_image_url": "https://example.com/profile.jpg",
                            }
                        ],
                        "media": [
                            {
                                "media_key": "3_1",
                                "type": "photo",
                                "url": "https://example.com/photo.jpg",
                                "alt_text": "Test image",
                            }
                        ],
                    },
                },
            )

        config = SimpleNamespace(
            x_bearer_token="test-bearer",
            x_consumer_key="",
            x_consumer_secret="",
            x_access_token="",
            x_access_token_secret="",
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            tweet = await fetch_tweet_from_official_api(client, config, "123")

        self.assertEqual(request_count, 1)
        self.assertEqual(tweet.text, "Official tweet")
        self.assertEqual(tweet.user_screen_name, "testuser")
        self.assertEqual(tweet.likes, 7)
        self.assertEqual(tweet.hashtags, ["furry"])
        self.assertEqual(tweet.media_urls, ["https://example.com/photo.jpg"])

    async def test_official_api_error_makes_no_retry_attempt(self) -> None:
        request_count = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            return httpx.Response(503, text="unavailable")

        config = SimpleNamespace(
            x_bearer_token="test-bearer",
            x_consumer_key="",
            x_consumer_secret="",
            x_access_token="",
            x_access_token_secret="",
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(TweetFetchError):
                await fetch_tweet_from_official_api(client, config, "123")

        self.assertEqual(request_count, 1)
