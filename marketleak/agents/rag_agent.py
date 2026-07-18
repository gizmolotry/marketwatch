import math
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import timezone

import dateparser
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


class RAGAgent:
    """Retrieve timestamped news evidence for a prediction-market anomaly."""

    _MAX_SEARCH_ATTEMPTS = 5
    # Evidence farther from the anomaly than this is context, not an actionable lead.
    MAX_ACTIONABLE_INTERVAL_HOURS = 30 * 24
    _MARKET_JARGON = frozenset(
        {
            "a",
            "an",
            "and",
            "are",
            "at",
            "be",
            "bet",
            "betting",
            "by",
            "contract",
            "did",
            "do",
            "does",
            "for",
            "from",
            "game",
            "handicap",
            "happen",
            "happens",
            "how",
            "in",
            "is",
            "line",
            "market",
            "match",
            "moneyline",
            "no",
            "odds",
            "of",
            "on",
            "or",
            "outcome",
            "over",
            "point",
            "points",
            "price",
            "prop",
            "props",
            "reg",
            "resolve",
            "resolves",
            "set",
            "spread",
            "than",
            "the",
            "time",
            "to",
            "total",
            "totals",
            "under",
            "versus",
            "vs",
            "wager",
            "was",
            "were",
            "what",
            "when",
            "where",
            "which",
            "who",
            "will",
            "win",
            "winning",
            "wins",
            "with",
            "would",
            "yes",
        }
    )
    _DOMAIN_KEYWORDS = (
        ("tennis", frozenset({"atp", "tennis", "wta"})),
        ("basketball", frozenset({"basketball", "nba", "ncaa", "wnba"})),
        ("baseball", frozenset({"baseball", "mlb"})),
        ("hockey", frozenset({"hockey", "nhl"})),
        ("soccer", frozenset({"champions", "fifa", "football", "laliga", "mls", "soccer", "uefa"})),
        ("esports", frozenset({"counterstrike", "dota", "esports", "leagueoflegends", "valorant"})),
        ("crypto", frozenset({"bitcoin", "btc", "crypto", "ethereum", "eth", "solana"})),
        ("politics", frozenset({"ballot", "congress", "election", "president", "senate"})),
        ("finance", frozenset({"earnings", "fed", "finance", "inflation", "rates", "stocks"})),
    )

    def __init__(self, model_name="all-MiniLM-L6-v2"):
        print("Initializing RAG Agent...")
        self.embedder = SentenceTransformer(model_name)

    @staticmethod
    def _clean_text(value):
        if not isinstance(value, str):
            return ""
        return " ".join(value.split())

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        return number if math.isfinite(number) else default

    @staticmethod
    def _finite_float_or_none(value):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @classmethod
    def _first_text(cls, market_row, *fields):
        for field in fields:
            text = cls._clean_text(market_row.get(field))
            if text:
                return text
        return ""

    @classmethod
    def _strip_specific_terms(cls, text):
        """Remove parenthetical odds, numeric lines, and punctuation."""
        text = cls._clean_text(text)
        if not text:
            return ""
        text = re.sub(r"\([^)]*\)", " ", text)
        text = re.sub(r"[+\-]?\d+(?:[.,]\d+)?%?", " ", text)
        text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
        text = text.replace("_", " ")
        return cls._clean_text(text)

    @classmethod
    def _entity_query(cls, text):
        """Retain likely event/entity terms while dropping market boilerplate."""
        simplified = cls._strip_specific_terms(text)
        tokens = [
            token
            for token in simplified.split()
            if token.casefold() not in cls._MARKET_JARGON
        ]
        return " ".join(tokens)

    @classmethod
    def _slug_text(cls, value):
        value = cls._clean_text(value)
        if not value:
            return ""
        value = urllib.parse.unquote(value)
        value = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)
        return cls._entity_query(value)

    @classmethod
    def _domain_hint(cls, market_row, question, slug):
        for field in ("sport", "category", "league", "domain", "topic", "event_type", "event_category"):
            hint = cls._entity_query(market_row.get(field))
            if hint and hint.casefold() not in {"general", "none", "other", "unknown"}:
                return " ".join(hint.split()[:3])

        searchable = cls._strip_specific_terms(f"{question} {slug}").casefold()
        tokens = set(searchable.split())
        compact = searchable.replace(" ", "")
        for domain, keywords in cls._DOMAIN_KEYWORDS:
            if tokens.intersection(keywords) or any(
                len(keyword) >= 8 and keyword in compact for keyword in keywords
            ):
                return domain
        return ""

    @staticmethod
    def _merge_query_terms(*parts):
        merged = []
        seen = set()
        for part in parts:
            for token in part.split():
                key = token.casefold()
                if key and key not in seen:
                    seen.add(key)
                    merged.append(token)
        return " ".join(merged)

    @classmethod
    def _build_search_attempts(cls, market_row):
        """Build a deterministic, deduplicated, and bounded fallback plan."""
        if not hasattr(market_row, "get"):
            market_row = {}

        question = cls._clean_text(market_row.get("question"))
        slug = cls._first_text(market_row, "slug", "market_slug", "id", "market_uid")

        attempts = []
        seen_queries = set()

        def add(strategy, query, evidence_scope):
            query = cls._clean_text(query)
            key = query.casefold()
            if not query or key in seen_queries or len(attempts) >= cls._MAX_SEARCH_ATTEMPTS:
                return
            seen_queries.add(key)
            attempts.append(
                {
                    "query": query,
                    "strategy": strategy,
                    "evidence_scope": evidence_scope,
                }
            )

        add("exact", question, "market_specific")

        simplified = cls._strip_specific_terms(question)
        add("simplified", simplified, "market_context")

        question_entities = cls._entity_query(simplified)
        add("entities", question_entities, "entity_context")

        slug_entities = cls._slug_text(slug)
        domain_hint = cls._domain_hint(market_row, question, slug)
        contextual_entities = cls._merge_query_terms(question_entities, slug_entities)
        if contextual_entities:
            contextual_query = cls._merge_query_terms(contextual_entities, domain_hint, "news")
            strategy = "entity_context" if question_entities else "slug_context"
            add(strategy, contextual_query, "entity_context")

        generic_query = f"latest {domain_hint} news" if domain_hint else "latest breaking news"
        add("general_context", generic_query, "general_context")
        return attempts

    def _parse_date(self, date_str):
        date_str = self._clean_text(date_str)
        if not date_str:
            return None
        try:
            parsed = dateparser.parse(date_str)
        except Exception:
            return None
        if not parsed:
            return None

        try:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            timestamp = float(parsed.timestamp())
        except Exception:
            return None
        return timestamp if math.isfinite(timestamp) else None

    def _fetch_google_news_rss(self, query: str):
        # Scrape Google News RSS (rate limit safe, robust)
        url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(query)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as response:
                xml_data = response.read()

            root = ET.fromstring(xml_data)
            results = []
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                pub_date = item.findtext("pubDate", "")
                desc = item.findtext("description", "")

                results.append(
                    {
                        "title": title,
                        "body": desc,
                        "url": link,
                        "date": pub_date,
                    }
                )
            return results
        except Exception as exc:
            print(f"[RAG] Error fetching RSS: {exc}")
            return []

    def _timestamped_chunks(self, results):
        """Validate timestamps/content and remove duplicate feed articles."""
        if not isinstance(results, (list, tuple)):
            return []

        chunks = []
        seen_urls = set()
        seen_texts = set()
        for result in results:
            if not hasattr(result, "get"):
                continue
            raw_date = result.get("date")
            timestamp = self._parse_date(raw_date)
            if timestamp is None:
                continue

            title = self._clean_text(result.get("title"))
            body = self._clean_text(result.get("body"))
            text = " - ".join(part for part in (title, body) if part)
            if not text:
                continue

            url = self._clean_text(result.get("url"))
            url_key = url.casefold()
            text_key = text.casefold()
            if (url_key and url_key in seen_urls) or text_key in seen_texts:
                continue
            if url_key:
                seen_urls.add(url_key)
            seen_texts.add(text_key)

            chunks.append(
                {
                    "text": text,
                    "url": url,
                    "timestamp": timestamp,
                    "date_str": raw_date,
                }
            )
        return chunks

    def fetch_and_score(self, market_row, top_k=5):
        """
        Fetch news for a market, retrieve relevant timestamped evidence, and
        calculate the Information Lead Distance (PPIM).

        Fallback evidence is returned for analyst/UI context but deliberately
        assigned a zero PPIM. Only an exact-query hit with sane timing can
        contribute an actionable score.
        """
        if not hasattr(market_row, "get"):
            market_row = {}

        question = self._clean_text(market_row.get("question"))
        slug = self._first_text(market_row, "slug", "market_slug", "id", "market_uid")
        parsed_shock_ts = self._finite_float_or_none(market_row.get("shock_timestamp"))
        shock_ts = parsed_shock_ts if parsed_shock_ts is not None else 0.0
        magnitude = self._safe_float(market_row.get("shock_magnitude", 0))
        attempts = self._build_search_attempts(market_row)

        selected_attempt = None
        valid_chunks = []
        for attempt_number, attempt in enumerate(attempts, start=1):
            search_query = attempt["query"]
            print(f"\n[RAG] Search {attempt_number}/{len(attempts)} ({attempt['strategy']}): {search_query[:80]}...")
            try:
                results = self._fetch_google_news_rss(search_query)
            except Exception as exc:
                # Tests/injected providers may not use the defensive RSS wrapper.
                print(f"[RAG] Search provider failed for '{search_query[:60]}': {exc}")
                continue

            valid_chunks = self._timestamped_chunks(results)
            if valid_chunks:
                selected_attempt = attempt
                break
            if results:
                print("[RAG] Results lacked a usable timestamp; broadening search.")
            else:
                print("[RAG] No results; broadening search.")

        if selected_attempt is None:
            print("[RAG] No usable timestamped news found after all fallback searches.")
            return None

        print(f"[RAG] Indexing {len(valid_chunks)} unique news articles in FAISS...")
        texts = [chunk["text"] for chunk in valid_chunks]
        ranking_text = question or self._slug_text(slug) or selected_attempt["query"]

        try:
            embeddings = np.asarray(self.embedder.encode(texts), dtype="float32")
            if embeddings.ndim == 1:
                embeddings = embeddings.reshape(1, -1)
            if embeddings.ndim != 2 or embeddings.shape[0] != len(valid_chunks) or embeddings.shape[1] == 0:
                raise ValueError("embedder returned an unexpected document embedding shape")

            index = faiss.IndexFlatL2(embeddings.shape[1])
            index.add(embeddings)

            query_embedding = np.asarray(self.embedder.encode([ranking_text]), dtype="float32")
            if query_embedding.ndim == 1:
                query_embedding = query_embedding.reshape(1, -1)
            if query_embedding.ndim != 2 or query_embedding.shape[1] != embeddings.shape[1]:
                raise ValueError("embedder returned an unexpected query embedding shape")

            try:
                requested_k = int(top_k)
            except (TypeError, ValueError):
                requested_k = 5
            retrieval_k = min(max(1, requested_k), len(valid_chunks))
            _distances, indices = index.search(query_embedding, retrieval_k)
        except Exception as exc:
            print(f"[RAG] Evidence indexing/retrieval failed: {exc}")
            return None

        best_idx = next(
            (
                int(candidate)
                for candidate in indices[0]
                if 0 <= int(candidate) < len(valid_chunks)
            ),
            None,
        )
        if best_idx is None:
            print(f"[RAG] FAISS failed to retrieve evidence for query: {ranking_text}")
            return None

        best_evidence = valid_chunks[best_idx]
        news_ts = best_evidence["timestamp"]
        if parsed_shock_ts is None:
            lead_time_hours = 0.0
            timing_valid = False
            timing_issue = "invalid_or_missing_shock_timestamp"
        else:
            lead_time_hours = (news_ts - parsed_shock_ts) / 3600.0
            timing_valid = abs(lead_time_hours) <= self.MAX_ACTIONABLE_INTERVAL_HOURS
            timing_issue = None if timing_valid else "shock_news_interval_exceeds_30_days"

        evidence_scope = selected_attempt["evidence_scope"]
        exact_market_evidence = selected_attempt["strategy"] == "exact" and evidence_scope == "market_specific"
        if not exact_market_evidence:
            ppim = 0.0
            ppim_suppression_reason = "fallback_evidence_not_actionable"
        elif not timing_valid:
            ppim = 0.0
            ppim_suppression_reason = timing_issue
        else:
            ppim = magnitude * lead_time_hours
            ppim_suppression_reason = None

        result = {
            "evidence_text": best_evidence["text"],
            "evidence_url": best_evidence["url"],
            "evidence_date": best_evidence["date_str"],
            "shock_timestamp": shock_ts,
            "news_timestamp": news_ts,
            "lead_time_hours": lead_time_hours,
            "shock_magnitude": magnitude,
            "ppim_score": ppim,
            "search_query": selected_attempt["query"],
            "search_strategy": selected_attempt["strategy"],
            "fallback_used": selected_attempt["strategy"] != "exact",
            "evidence_scope": evidence_scope,
            "timing_valid": timing_valid,
            "timing_issue": timing_issue,
            "ppim_suppression_reason": ppim_suppression_reason,
        }

        if ppim_suppression_reason:
            print(f"[RAG] Context found; PPIM suppressed ({ppim_suppression_reason}).")
        else:
            print(f"[RAG] Top evidence found! PPIM Score: {ppim:.2f}")
        return result
