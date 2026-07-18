from datetime import datetime, timezone

import numpy as np
import pytest

from marketleak.agents.rag_agent import RAGAgent


VALID_DATE = "Mon, 01 Jan 2024 12:00:00 GMT"


class FakeEmbedder:
    def __init__(self):
        self.calls = []

    def encode(self, texts):
        values = list(texts)
        self.calls.append(values)
        return np.asarray(
            [
                [
                    float(len(value)),
                    float(sum(ord(char) for char in value) % 997),
                ]
                for value in values
            ],
            dtype="float32",
        )


def make_agent():
    agent = object.__new__(RAGAgent)
    agent.embedder = FakeEmbedder()
    return agent


def article(*, title="Relevant report", body="Event details", url="https://example.test/story", date=VALID_DATE):
    return {"title": title, "body": body, "url": url, "date": date}


def market_row(question="Will Example Event happen?", slug="example-event"):
    return {
        "question": question,
        "slug": slug,
        "shock_timestamp": 1_704_067_200,
        "shock_magnitude": 2.0,
    }


def test_exact_success_uses_one_request_and_preserves_result_contract(monkeypatch):
    agent = make_agent()
    calls = []

    def fetch(query):
        calls.append(query)
        return [article()]

    monkeypatch.setattr(agent, "_fetch_google_news_rss", fetch)
    row = market_row(question="Will Example Event happen?")

    result = agent.fetch_and_score(row)

    assert calls == ["Will Example Event happen?"]
    assert result is not None
    assert {
        "evidence_text",
        "evidence_url",
        "evidence_date",
        "shock_timestamp",
        "news_timestamp",
        "lead_time_hours",
        "shock_magnitude",
        "ppim_score",
    } <= result.keys()
    assert result["search_query"] == row["question"]
    assert result["search_strategy"] == "exact"
    assert result["fallback_used"] is False
    assert result["evidence_scope"] == "market_specific"
    assert result["timing_valid"] is True
    assert result["timing_issue"] is None
    assert result["ppim_suppression_reason"] is None
    assert result["ppim_score"] > 0.0
    assert agent.embedder.calls[-1] == [row["question"]]


def test_specific_handicap_falls_through_to_entity_query(monkeypatch):
    agent = make_agent()
    calls = []
    question = "Set Handicap: Gorgodze (-1.5) vs Heide (+1.5)"

    def fetch(query):
        calls.append(query)
        return [article(title="Gorgodze faces Heide")] if query == "Gorgodze Heide" else []

    monkeypatch.setattr(agent, "_fetch_google_news_rss", fetch)

    result = agent.fetch_and_score(market_row(question=question, slug="gorgodze-vs-heide"))

    assert calls == [
        question,
        "Set Handicap Gorgodze vs Heide",
        "Gorgodze Heide",
    ]
    assert result is not None
    assert result["search_query"] == "Gorgodze Heide"
    assert result["search_strategy"] == "entities"
    assert result["fallback_used"] is True
    assert result["evidence_scope"] == "entity_context"
    assert result["ppim_score"] == 0.0
    assert result["ppim_suppression_reason"] == "fallback_evidence_not_actionable"
    assert agent.embedder.calls[-1] == [question]


def test_invalid_dates_cause_search_to_continue(monkeypatch):
    agent = make_agent()
    calls = []
    question = "Team Alpha (-1.5) vs Team Beta (+1.5)"

    def fetch(query):
        calls.append(query)
        if query == question:
            return [article(date="not a publication date")]
        if query == "Team Alpha vs Team Beta":
            return [article(title="Alpha plays Beta")]
        return []

    monkeypatch.setattr(agent, "_fetch_google_news_rss", fetch)

    result = agent.fetch_and_score(market_row(question=question))

    assert calls == [question, "Team Alpha vs Team Beta"]
    assert result is not None
    assert result["search_strategy"] == "simplified"
    assert result["search_query"] == "Team Alpha vs Team Beta"
    assert result["evidence_scope"] == "market_context"
    assert result["ppim_score"] == 0.0


def test_generated_search_queries_are_deduplicated_and_bounded():
    attempts = RAGAgent._build_search_attempts(
        {"question": "Gorgodze Heide", "slug": "gorgodze-heide"}
    )
    queries = [attempt["query"] for attempt in attempts]

    assert queries == ["Gorgodze Heide", "Gorgodze Heide news", "latest breaking news"]
    assert len(queries) == len({query.casefold() for query in queries})
    assert len(queries) <= RAGAgent._MAX_SEARCH_ATTEMPTS


@pytest.mark.parametrize("question", [None, "   ", 123])
def test_missing_or_non_string_question_uses_slug(monkeypatch, question):
    agent = make_agent()
    calls = []

    def fetch(query):
        calls.append(query)
        return [article(title="Gorgodze and Heide update")] if query == "gorgodze heide news" else []

    monkeypatch.setattr(agent, "_fetch_google_news_rss", fetch)

    result = agent.fetch_and_score(market_row(question=question, slug="gorgodze-heide"))

    assert calls == ["gorgodze heide news"]
    assert result is not None
    assert result["search_strategy"] == "slug_context"
    assert result["search_query"] == "gorgodze heide news"
    assert result["evidence_scope"] == "entity_context"
    assert result["ppim_score"] == 0.0
    assert agent.embedder.calls[-1] == ["gorgodze heide"]


def test_generic_fallback_returns_context_with_zero_ppim(monkeypatch):
    agent = make_agent()
    calls = []

    def fetch(query):
        calls.append(query)
        if query == "latest breaking news":
            return [article(title="General breaking news")]
        return []

    monkeypatch.setattr(agent, "_fetch_google_news_rss", fetch)

    result = agent.fetch_and_score(
        market_row(
            question="Set Handicap: Gorgodze (-1.5) vs Heide (+1.5)",
            slug="gorgodze-vs-heide",
        )
    )

    assert result is not None
    assert calls[-1] == "latest breaking news"
    assert result["search_strategy"] == "general_context"
    assert result["evidence_scope"] == "general_context"
    assert result["fallback_used"] is True
    assert result["ppim_score"] == 0.0
    assert result["lead_time_hours"] != 0.0


@pytest.mark.parametrize("shock_timestamp", [None, float("nan"), float("inf"), float("-inf"), "invalid"])
def test_invalid_shock_timestamp_neutralizes_lead_and_ppim(monkeypatch, shock_timestamp):
    agent = make_agent()
    monkeypatch.setattr(agent, "_fetch_google_news_rss", lambda _query: [article()])
    row = market_row()
    row["shock_timestamp"] = shock_timestamp

    result = agent.fetch_and_score(row)

    assert result is not None
    assert result["shock_timestamp"] == 0.0
    assert result["lead_time_hours"] == 0.0
    assert result["ppim_score"] == 0.0
    assert result["timing_valid"] is False
    assert result["timing_issue"] == "invalid_or_missing_shock_timestamp"
    assert result["ppim_suppression_reason"] == "invalid_or_missing_shock_timestamp"


@pytest.mark.parametrize(
    ("lead_hours", "expected_timing_valid"),
    [
        (RAGAgent.MAX_ACTIONABLE_INTERVAL_HOURS, True),
        (RAGAgent.MAX_ACTIONABLE_INTERVAL_HOURS + 1, False),
    ],
)
def test_actionable_ppim_has_a_thirty_day_temporal_bound(monkeypatch, lead_hours, expected_timing_valid):
    agent = make_agent()
    monkeypatch.setattr(agent, "_fetch_google_news_rss", lambda _query: [article()])
    news_timestamp = datetime(2024, 1, 1, 12, tzinfo=timezone.utc).timestamp()
    row = market_row()
    row["shock_timestamp"] = news_timestamp - lead_hours * 3600

    result = agent.fetch_and_score(row)

    assert result is not None
    assert result["timing_valid"] is expected_timing_valid
    if expected_timing_valid:
        assert result["ppim_score"] == row["shock_magnitude"] * lead_hours
        assert result["ppim_suppression_reason"] is None
    else:
        assert result["ppim_score"] == 0.0
        assert result["timing_issue"] == "shock_news_interval_exceeds_30_days"
        assert result["ppim_suppression_reason"] == "shock_news_interval_exceeds_30_days"


def test_parse_date_treats_naive_feed_dates_as_utc():
    agent = make_agent()

    timestamp = agent._parse_date("2024-01-01 12:00:00")

    assert timestamp == datetime(2024, 1, 1, 12, tzinfo=timezone.utc).timestamp()


def test_parse_date_handles_timestamp_conversion_failure(monkeypatch):
    agent = make_agent()

    class UnconvertibleDate:
        tzinfo = None

        def replace(self, **_kwargs):
            return self

        def timestamp(self):
            raise OSError("date is outside the platform timestamp range")

    monkeypatch.setattr("marketleak.agents.rag_agent.dateparser.parse", lambda _value: UnconvertibleDate())

    assert agent._parse_date("Fri, 31 Dec 9999 23:59:59 GMT") is None


def test_duplicate_articles_are_indexed_once(monkeypatch):
    agent = make_agent()
    duplicate = article()
    monkeypatch.setattr(
        agent,
        "_fetch_google_news_rss",
        lambda _query: [duplicate, dict(duplicate), article(url="", title="Relevant report")],
    )

    result = agent.fetch_and_score(market_row())

    assert result is not None
    assert len(agent.embedder.calls[0]) == 1


def test_total_provider_failure_returns_none(monkeypatch):
    agent = make_agent()
    calls = []

    def failing_fetch(query):
        calls.append(query)
        raise OSError("provider unavailable")

    monkeypatch.setattr(agent, "_fetch_google_news_rss", failing_fetch)

    result = agent.fetch_and_score(
        market_row(
            question="Set Handicap: Gorgodze (-1.5) vs Heide (+1.5)",
            slug="gorgodze-vs-heide",
        )
    )

    assert result is None
    assert calls[-1] == "latest breaking news"
    assert len(calls) <= RAGAgent._MAX_SEARCH_ATTEMPTS
    assert len(calls) == len({query.casefold() for query in calls})
