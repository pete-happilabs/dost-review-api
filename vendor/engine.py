"""
DOST Reputation Engine — Incremental AI processor.

One public function:
  process_reputation(profile) -> (output, metrics)

Input: one profile object {profileId, about, reviews:[...NEW only...], state}
       (a list of profiles is also accepted → list of results)
Output: (reputation JSON incl. updated `state`, token metrics) as siblings —
        mirrors DAS/DPA `response, metrics = ...`. metrics is {"models": [...]}.

Incremental & O(1) per new review: the engine reads only the NEW review(s) and
folds them into a compressed `state` (per-tag decayed accumulators) that the
backend stores and echoes back. A million reviews compress into ~#tags counters
— it never re-reads history. The engine itself keeps nothing between calls.

Scoring honours time decay: older evidence counts for less, and negative
signals fade slower than positive ones (see *_HALFLIFE_DAYS).
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import anthropic

logger = logging.getLogger(__name__)

# ── Tags ─────────────────────────────────────────────────────────────────────
# Curated tag list. AI can only pick from these.
# AI scores INTENSITY only (0-5). Code handles polarity.

POSITIVE_TAGS = [
    "#on-time-delivery", "#product-quality", "#hygiene", "#fair-pricing",
    "#respectful", "#reliable", "#honest", "#responsive", "#authentic",
    "#transparent", "#safe-driving", "#clean-vehicle", "#skilled",
    "#knowledgeable", "#professional", "#punctual", "#accurate",
    "#empathetic", "#effective", "#helpful", "#friendly", "#patient",
    "#good-packaging", "#fresh-food", "#tasty", "#generous-portions",
    "#good-value", "#fast-service", "#clean-place", "#comfortable",
    "#well-maintained", "#good-communication", "#listens-well",
    "#problem-solved", "#easy-to-use", "#convenient",
]

NEGATIVE_TAGS = [
    "#rude", "#late", "#overpriced", "#unhygienic", "#scam",
    "#harassment", "#fraud", "#fake", "#spammy", "#no-show",
    "#damaged", "#wrong-order", "#cold-food", "#stale",
    "#overcharging", "#reckless-driving", "#dirty", "#slow",
    "#unresponsive", "#dishonest", "#misleading", "#poor-quality",
    "#broken", "#missing-items", "#bad-taste", "#too-spicy",
    "#undercooked", "#raw", "#expired",
]

ALL_TAGS = POSITIVE_TAGS + NEGATIVE_TAGS
NEGATIVE_TAG_SET = set(NEGATIVE_TAGS)


def _plain(tag: str) -> str:
    """Drop the leading '#' — tags are plain names everywhere outside extraction."""
    return tag[1:] if tag.startswith("#") else tag


# Plain-name negative set — tags are stored/scored without the '#'.
NEGATIVE_PLAIN = {_plain(t) for t in NEGATIVE_TAGS}

# Bayesian prior — blends toward this with low sample sizes.
# New profiles with no reviews start here (spec: normal = 3.0).
PRIOR_SCORE = 3.0
PRIOR_WEIGHT = 3  # acts like 3 virtual reviews at 3.0

# ── Time decay ───────────────────────────────────────────────────────────────
# Older reviews count for less. Negative signals fade slower than positive
# ones, so bad behaviour keeps dragging reputation down long after the fact.
# Weight = 0.5 ** (age_days / half_life): a review is worth half as much once
# it is one half-life old.
POSITIVE_HALFLIFE_DAYS = 180.0   # positive reviews halve in ~6 months
NEGATIVE_HALFLIFE_DAYS = 540.0   # negative reviews halve in ~18 months (3x slower)


# ── Tiers ────────────────────────────────────────────────────────────────────
# Spec tiers (0-5 scale):
#   excellent 4.0-5.0 | very good 3.0-3.9 | average 2.0-2.9 | bad 1.0-1.9 | very bad 0.0-0.9

def _get_score_tier(score: float) -> str:
    """Map a score (0-5) to a tier label. Single source of truth for labels."""
    if score >= 4.0:
        return "excellent"
    elif score >= 3.0:
        return "very good"
    elif score >= 2.0:
        return "average"
    elif score >= 1.0:
        return "bad"
    else:
        return "very bad"


# ── AI Client ────────────────────────────────────────────────────────────────

_client = None

def _get_client() -> anthropic.Anthropic:
    """Reuse a single client instance."""
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    return _client


# ── Token metrics ────────────────────────────────────────────────────────────
# Same wire shape DAS/DPA emit: an array of per-model entries. Cache keys are
# only present when nonzero (this engine doesn't use prompt caching, so they
# never appear). Tokens are keyed by the EXACT model id the API returns.

class _TokenMeter:
    """Accumulate exact per-model token usage across all AI calls."""

    def __init__(self) -> None:
        self.models: Dict[str, Dict[str, int]] = {}

    def record(self, response: Any) -> None:
        """Add one Anthropic response's usage, keyed by its exact model id."""
        model = getattr(response, "model", "") or ""
        usage = getattr(response, "usage", None)
        if not model or usage is None:
            return
        entry = self.models.setdefault(model, {"input_tokens": 0, "output_tokens": 0})
        entry["input_tokens"] += int(getattr(usage, "input_tokens", 0) or 0)
        entry["output_tokens"] += int(getattr(usage, "output_tokens", 0) or 0)

    def to_dict(self) -> Dict[str, Any]:
        """DAS/DPA wire shape: {"models": [{"model", "input_tokens", "output_tokens"}]}."""
        return {
            "models": [
                {"model": model, "input_tokens": t["input_tokens"], "output_tokens": t["output_tokens"]}
                for model, t in self.models.items()
                if t["input_tokens"] > 0 or t["output_tokens"] > 0
            ]
        }


# ── Time decay helpers ───────────────────────────────────────────────────────

def _parse_iso(ts: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp (handles a trailing 'Z'). None on failure."""
    if not isinstance(ts, str) or not ts.strip():
        return None
    try:
        dt = datetime.fromisoformat(ts.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _review_age_days(review: Dict[str, Any], now: datetime) -> float:
    """Age of a review in days. 0.0 when no usable timestamp (no decay)."""
    ts = review.get("createdAt") or review.get("timestamp") or review.get("created_at")
    dt = _parse_iso(ts)
    if dt is None:
        return 0.0
    return max(0.0, (now - dt).total_seconds() / 86400.0)


def _recency_weight(age_days: float, is_negative: bool) -> float:
    """Exponential decay weight in (0, 1]. Negatives decay slower than positives."""
    if age_days <= 0:
        return 1.0
    half_life = NEGATIVE_HALFLIFE_DAYS if is_negative else POSITIVE_HALFLIFE_DAYS
    return 0.5 ** (age_days / half_life)


# ── Extraction ───────────────────────────────────────────────────────────────

_POSITIVE_LIST = ", ".join(POSITIVE_TAGS)
_NEGATIVE_LIST = ", ".join(NEGATIVE_TAGS)

_EXTRACTION_SYSTEM = """You are a review classifier. Given a customer review, identify which tags apply and score the INTENSITY of evidence for each tag.

Score scale (always means intensity, regardless of tag type):
  0 = no evidence
  1 = slight/minor mention
  2 = noticeable
  3 = clear evidence
  4 = strong evidence
  5 = overwhelming/extreme evidence

Rules:
1. ONLY use tags from the provided lists.
2. Only include tags the review gives clear evidence for.
3. For ALL tags (positive and negative), score means how strongly the review supports that tag.
4. Return valid JSON only: {"#tag": score, ...}
5. If nothing applies, return {}."""


def _extract_tags_from_review(
    review_text: str, meter: Optional["_TokenMeter"] = None
) -> Optional[Dict[str, float]]:
    """AI reads one review, picks tags and scores intensity 0-5.

    Returns dict of {tag: score} on success, None on failure.
    None means extraction failed (not the same as empty {} which means no tags found).
    """
    # Delimit review text as data to reduce injection risk
    user_msg = f"""Positive tags: {_POSITIVE_LIST}

Negative tags: {_NEGATIVE_LIST}

<review>
{review_text}
</review>

Classify this review. Return JSON only."""

    try:
        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=_EXTRACTION_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        if meter is not None:
            meter.record(response)  # count tokens even if parsing fails below

        text = response.content[0].text.strip()
        # Handle markdown fences
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            text = text.rsplit("```", 1)[0].strip()

        tags = json.loads(text)
        if not isinstance(tags, dict):
            logger.error(f"AI returned non-dict: {type(tags)}")
            return None

        valid_set = set(ALL_TAGS)
        return {
            tag: max(0.0, min(5.0, float(score)))
            for tag, score in tags.items()
            if tag in valid_set
        }
    except json.JSONDecodeError as e:
        logger.error(f"Tag extraction JSON parse failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Tag extraction failed: {e}")
        return None


def _generate_summary(
    profile_id: str,
    about: str,
    top_tags: List[Dict],
    total_reviews: int,
    overall_score: float,
    reputation: str,
    meter: Optional["_TokenMeter"] = None,
) -> str:
    """AI writes a 2-4 line summary using only the top 5 tags."""
    tag_lines = []
    for t in top_tags:
        polarity = "negative" if t["tag"] in NEGATIVE_PLAIN else "positive"
        tag_lines.append(
            f"  {t['tag']} ({polarity}): intensity {t['score']}/5, "
            f"mentioned by {t['weight']} of {total_reviews} reviewers"
        )

    prompt = f"""Write a 2-4 line summary of this profile's reputation.

Profile: {profile_id}{f" — {about}" if about else ""}
Reputation: {reputation} ({overall_score}/5)
Total reviews: {total_reviews}

Top tags:
{chr(10).join(tag_lines)}

Note: negative tags (like #rude, #late) mean bad things. Higher intensity = worse behavior.
Write in plain english. Be specific, mention numbers. Only talk about these tags. No fluff."""

    try:
        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        if meter is not None:
            meter.record(response)
        return response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        return ""


# ── Scoring ──────────────────────────────────────────────────────────────────
#
# Per-tag accumulators carried in `state` (backend stores + echoes back):
#   count        — how many reviews mentioned the tag (integer, no decay)
#   intensitySum — sum of raw intensities (for the displayed per-tag avg score)
#   w            — decayed weight  = Σ recency_weight
#   s            — decayed score   = Σ (polarity-adjusted intensity · recency_weight)
# w / s are anchored at state.lastUpdated; they're decayed forward to "now" on
# each call, so a million reviews compress into ≤ (#tags) tiny counters — O(1)
# per new review, and negatives fade slower than positives.


def _decay_state(tag_states: Dict[str, Dict[str, float]], age_days: float) -> None:
    """Age every tag's decayed accumulators (w, s) forward by `age_days`."""
    if age_days <= 0:
        return
    for tag, st in tag_states.items():
        f = _recency_weight(age_days, tag in NEGATIVE_PLAIN)
        st["w"] = st.get("w", 0.0) * f
        st["s"] = st.get("s", 0.0) * f


def _score_from_top(top: List) -> float:
    """Decay-weighted score over the top tags, blended toward the 3.0 prior.

    `top` is a list of (tag, state) pairs. A strong recent #scam pulls the score
    down (its intensity was inverted into `s` at fold-in time) and keeps pulling
    as it ages slowly. Thin / stale evidence blends toward PRIOR_SCORE.
    """
    total_w = sum(st.get("w", 0.0) for _, st in top)
    if total_w <= 0:
        return PRIOR_SCORE
    total_s = sum(st.get("s", 0.0) for _, st in top)
    raw = total_s / total_w
    blended = (raw * total_w + PRIOR_SCORE * PRIOR_WEIGHT) / (total_w + PRIOR_WEIGHT)
    return round(max(0.0, min(5.0, blended)), 1)


# ── Main Function ────────────────────────────────────────────────────────────

def _error_result(profile_id: str, message: str) -> Dict[str, Any]:
    """Error payload — no reputation/score/tags/state, just the failure.

    Shape mirrors the protocol's dostError ({message, willRetry}). The backend
    detects a failed profile by the presence of `error` and leaves its stored
    state untouched.
    """
    return {"profileId": profile_id, "error": {"message": message, "willRetry": True}}


def process_reputation(data: Any) -> Any:
    """Update a profile's reputation from its NEW reviews. Returns ``(output, metrics)``.

    Like DAS/DPA engine calls (``response, metrics = ...``), the reputation
    payload and the token metrics come back as siblings, not nested:
        output, metrics = process_reputation(profile)
    `output` is one result dict (or a list, if a list of profiles was passed);
    `metrics` is the per-model token usage for the whole call ({"models": [...]}).

    Incremental & O(1) per new review — it does NOT re-read the full history:
      - `profile["reviews"]` is only the NEW review(s) since last time.
      - `profile["state"]` is the compressed history the backend stored last
        call (per-tag decayed accumulators + lastUpdated + totalReviews). New
        profile → omit it. The backend stores it opaquely and echoes it back;
        it never has to parse it.
      - Each call decays the stored accumulators forward to now, folds in only
        the new review(s), and returns the updated `state` inside `output`.
    """
    now = datetime.now(timezone.utc)
    meter = _TokenMeter()  # exact per-model token usage for the whole call
    if isinstance(data, list):
        output = [_process_one(p, now, meter) for p in data]
    else:
        output = _process_one(data, now, meter)
    return output, meter.to_dict()


def _process_one(profile: Dict[str, Any], now: datetime, meter: "_TokenMeter") -> Dict[str, Any]:
    profile_id = profile.get("profileId", "")
    about = (profile.get("about") or "").strip()
    new_reviews = profile.get("reviews", []) or []

    # Load the compressed history the backend echoed back (empty for new profile).
    state = profile.get("state") or {}
    tag_states: Dict[str, Dict[str, float]] = {
        tag: dict(st) for tag, st in (state.get("tagStates") or {}).items()
    }
    total_reviews = int(state.get("totalReviews") or 0)

    # 1) Age the stored accumulators forward to now (before folding in new ones).
    last_updated = _parse_iso(state.get("lastUpdated"))
    if last_updated is not None:
        _decay_state(tag_states, max(0.0, (now - last_updated).total_seconds() / 86400.0))

    # 2) Fold in ONLY the new review(s). A tag counts once per review — the AI
    #    returns each tag at most once per review, so "bad bad bad" is still one.
    for review in new_reviews:
        review_text = review.get("description", "").strip()
        if not review_text:
            continue
        tags = _extract_tags_from_review(review_text, meter)
        if tags is None:
            # AI couldn't read the review. Don't fabricate a reputation and don't
            # touch the stored state — return an error so the backend keeps the
            # old state and can retry. (None = failure; {} = genuinely no tags.)
            return _error_result(profile_id, "tag extraction failed")
        total_reviews += 1
        if not tags:  # {} — review had no taggable content, still a real review
            continue
        age_days = _review_age_days(review, now)  # ~0 for a fresh review
        for raw_tag, intensity in tags.items():
            tag = _plain(raw_tag)  # store/score without the '#'
            is_neg = tag in NEGATIVE_PLAIN
            rw = _recency_weight(age_days, is_neg)
            adjusted = (5.0 - intensity) if is_neg else intensity
            st = tag_states.setdefault(tag, {"count": 0, "intensitySum": 0.0, "w": 0.0, "s": 0.0})
            st["count"] += 1
            st["intensitySum"] += intensity
            st["w"] += rw
            st["s"] += adjusted * rw

    # 3) Rank by mention count (repetitiveness). All keys are plain names.
    ranked = sorted(
        (kv for kv in tag_states.items() if kv[1]["count"] > 0),
        key=lambda kv: kv[1]["count"], reverse=True,
    )
    top = ranked[:5]

    overall_score = _score_from_top(top)
    reputation = _get_score_tier(overall_score)

    # Displayed per-tag score = plain average intensity reviewers gave (0-5).
    top_tags = [
        {"tag": tag, "weight": st["count"], "score": round(st["intensitySum"] / st["count"], 1)}
        for tag, st in top
    ]
    all_tags = {tag: st["count"] for tag, st in ranked}

    summary = _generate_summary(
        profile_id, about, top_tags, total_reviews, overall_score, reputation, meter
    ) if top else ""

    return {
        "profileId": profile_id,
        "reputation": reputation,
        "reputationScore": overall_score,
        "totalReviews": total_reviews,
        "topTags": top_tags,
        "allTags": all_tags,
        "summary": summary,
        "state": {
            "lastUpdated": now.isoformat(),
            "totalReviews": total_reviews,
            "tagStates": tag_states,
        },
        "createdAt": now.isoformat(),
    }
