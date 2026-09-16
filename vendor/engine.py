"""
DOST Reputation Engine — Incremental AI processor.

Public functions:
  process_reputation(profile)  -> (output, metrics)   reviews → reputation (described below)
  process_conversation(data)   -> (output, metrics)   fraud family — see `# ── Fraud family`
  process_signals(data)        -> (output, metrics)   fraud family — see `# ── Fraud family`

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
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

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


# ── Fraud family ─────────────────────────────────────────────────────────────
# A second tag family with its own half-lives, fed by per-message signals from
# the intent gate rather than by reviews. Same accumulator shape as tagStates
# ({count, w}), same opaque-blob contract (caller stores and echoes `state`),
# same once-per-unit counting (once per CONVERSATION here, once per review there).
#
# Velocity tags describe HOW an account behaves and burst; outcome tags describe
# what HAPPENED to counterparties and are the dominant evidence. Research doc §3b.

FRAUD_VELOCITY_TAGS = [
    "channel_shift_early",          # channel_shift_request at turn ≤ 2
    "contact_share_pre_milestone",  # phone/handle before price_agreed
    "money_ask_no_milestone",       # money_ask before meet_scheduled / ledger_credit
    "fee_escalation",               # second money_ask by the same asker after a ledger_credit
    "jumped_deposit",               # small ledger_credit then money_ask/payee_asked_to_act ≤ 15 min
    "template_reuse",               # same text hash to ≥ N distinct counterparties
    "unsolicited_first_contact",    # first message of a thread from this account, no listing context
    "identity_then_money",          # identity_claim followed by money_ask before any milestone
    "categorical_block",            # the gate rejected on payment_manipulation / otp / phishing
]

FRAUD_OUTCOME_TAGS = [
    "counterparty_reported",        # the other party reported this account
    "paid_not_delivered",
    "not_received",
    "dispute_lost",
    "counterparty_silent_after_money_ask",
    "confirmed_fraud",              # human decision
    "shared_instrument_with_confirmed",  # VPA / device / ID-hash shared with a confirmed account
    "cleared_by_review",            # human decision the OTHER way — carries a NEGATIVE weight
]

FRAUD_TAGS = FRAUD_VELOCITY_TAGS + FRAUD_OUTCOME_TAGS
FRAUD_OUTCOME_SET = set(FRAUD_OUTCOME_TAGS)

FRAUD_VELOCITY_HALFLIFE_DAYS = 7.0
FRAUD_OUTCOME_HALFLIFE_DAYS = NEGATIVE_HALFLIFE_DAYS   # 540 — outcomes fade like #scam reviews

# Starting weights (research doc §3e). Inferred, to be tuned on DOST labels.
FRAUD_WEIGHTS = {
    "channel_shift_early": 1.0,
    "contact_share_pre_milestone": 1.0,
    "money_ask_no_milestone": 2.0,
    "fee_escalation": 6.0,
    "jumped_deposit": 6.0,
    "template_reuse": 2.0,
    "unsolicited_first_contact": 0.5,
    "identity_then_money": 3.0,
    "categorical_block": 4.0,
    "counterparty_reported": 5.0,
    "paid_not_delivered": 8.0,
    "not_received": 8.0,
    "dispute_lost": 8.0,
    "counterparty_silent_after_money_ask": 3.0,
    "confirmed_fraud": 14.0,   # w=1 on an established account → risk 82.6: critical on its own
    "shared_instrument_with_confirmed": 8.0,
    # A human reviewer clearing an account must move the score DOWN, otherwise the
    # queue is decorative. Negative mass is clamped to 0 in _risk_score.
    "cleared_by_review": -6.0,
}

# Score shaping. risk = 100 * (1 - exp(-mass / RISK_SCALE)); RISK_SCALE is the
# decayed weighted mass at which risk ≈ 63.
RISK_SCALE = 8.0
RISK_TIERS = (("none", 30.0), ("elevated", 60.0), ("high", 80.0), ("critical", 101.0))
# Tenure prior: a brand-new account's evidence is discounted; mirrors PRIOR_WEIGHT.
RISK_TENURE_PRIOR_DAYS = 14.0


def _fraud_recency_weight(age_days: float, tag: str) -> float:
    if age_days <= 0:
        return 1.0
    half = FRAUD_OUTCOME_HALFLIFE_DAYS if tag in FRAUD_OUTCOME_SET else FRAUD_VELOCITY_HALFLIFE_DAYS
    return 0.5 ** (age_days / half)


# Stage names the machine records the FIRST occurrence of. Signals come from the
# gate's record; milestones come from DOST's own systems via the caller.
_CONV_SIGNAL_STAGES = (
    "channel_shift_request", "identity_claim", "money_ask", "payee_asked_to_act",
    "payment_claim", "contact_share", "refusal_to_meet",
)
_CONV_MILESTONES = ("price_agreed", "meet_scheduled", "ledger_credit", "ledger_credit_small", "handover_confirmed")
_JUMPED_DEPOSIT_WINDOW_S = 15 * 60


def _fraud_error(conversation_id: str, message: str) -> Dict[str, Any]:
    return {"conversationId": conversation_id, "error": {"message": message, "willRetry": False}}


def process_conversation(data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Fold new events into one thread's stage state; derive sequence tags.

    Input:  {conversationId, state|None, events: [{ts, senderId, signals: [{name, confidence}],
             milestones: [name], textHash?, turnIndex?}]}  — events in order, NEW ones only.
    Output: ({conversationId, derivedTags: [{tag, profileId, ts}], state}, metrics)

    Pure code, no AI; metrics is always {"models": []}. A derived tag fires at
    most once per (conversation, sender) — the account layer counts per
    conversation, so firing it twice for the same sender would double count.
    """
    conversation_id = str(data.get("conversationId") or "")
    events = data.get("events")
    if not isinstance(events, list):
        return _fraud_error(conversation_id, "events must be a list"), {"models": []}

    prev = data.get("state")
    if prev is None:
        prev = {}
    elif not isinstance(prev, dict):
        return _fraud_error(conversation_id, "state must be an object"), {"models": []}
    # Echoed state is caller-stored bytes: every container is shape-checked so corruption degrades
    # to "empty", never to a raise. Only dict stages, str keys and a dict asker map are trusted.
    raw_stages = prev.get("stages")
    stages: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in (raw_stages if isinstance(raw_stages, dict) else {}).items()
                                          if isinstance(v, dict)}
    raw_fired = prev.get("fired")
    fired = {x for x in raw_fired if isinstance(x, str)} if isinstance(raw_fired, list) else set()
    raw_claimants = prev.get("identityClaimants")
    claimants = {x for x in raw_claimants if isinstance(x, str)} if isinstance(raw_claimants, list) else set()
    raw_askers = prev.get("moneyAskers")
    askers: Dict[str, str] = dict(raw_askers) if isinstance(raw_askers, dict) else {}
    if "moneyAskers" not in prev and "money_ask" in stages:
        # Legacy state predates the per-sender asker map: seed it from the first
        # recorded ask so fee_escalation still fires for the original asker. A
        # stage with no sender cannot seed it — skip, never raise.
        st = stages["money_ask"]
        if st.get("by") and st.get("ts"):
            askers = {st["by"]: st["ts"]}
    try:
        turns = int(prev.get("turns") or 0)
    except (TypeError, ValueError, OverflowError):   # int(inf) is OverflowError
        turns = 0
    derived: List[Dict[str, Any]] = []

    def stage_ts(name: str) -> Optional[datetime]:
        st = stages.get(name)
        return _parse_iso(st.get("ts")) if isinstance(st, dict) else None

    def fire(tag: str, profile_id: str, ts: str) -> None:
        key = f"{tag}|{profile_id}"
        if key in fired:
            return
        fired.add(key)
        derived.append({"tag": tag, "profileId": profile_id, "ts": ts})

    last_ts: Optional[str] = None
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ts = ev.get("ts")
        when = _parse_iso(ts)
        if when is None:
            logger.warning("process_conversation %s: skipping event with unparseable ts %r", conversation_id, ts)
            continue
        last_ts = ts
        sender = str(ev.get("senderId") or "")
        turns += 1
        turn = ev.get("turnIndex") if isinstance(ev.get("turnIndex"), int) else turns

        # Non-list signals/milestones are ignored: a bare int would raise, a bare string would
        # iterate its characters.
        raw_sigs = ev.get("signals")
        raw_ms = ev.get("milestones")
        names = [s.get("name") for s in raw_sigs if isinstance(s, dict)] if isinstance(raw_sigs, list) else []
        milestones = [m for m in raw_ms if m in _CONV_MILESTONES] if isinstance(raw_ms, list) else []
        if "identity_claim" in names:
            claimants.add(sender)

        for m in milestones:
            stages.setdefault(m, {"ts": ts, "turn": turn, "by": sender})

        for name in names:
            if name in _CONV_SIGNAL_STAGES:
                stages.setdefault(name, {"ts": ts, "turn": turn, "by": sender})

        met = stage_ts("meet_scheduled") or stage_ts("ledger_credit") or stage_ts("handover_confirmed")
        priced = stage_ts("price_agreed")

        if "money_ask" in names:
            askers.setdefault(sender, ts)
            if met is None:
                fire("money_ask_no_milestone", sender, ts)
                if sender in claimants:
                    fire("identity_then_money", sender, ts)
            credit = stage_ts("ledger_credit")
            first_ask = _parse_iso(askers.get(sender))
            if credit is not None and first_ask is not None and first_ask < credit <= when:
                fire("fee_escalation", sender, ts)

        if "payee_asked_to_act" in names or "money_ask" in names:
            small = stage_ts("ledger_credit_small")
            if small is not None and 0 <= (when - small).total_seconds() <= _JUMPED_DEPOSIT_WINDOW_S:
                fire("jumped_deposit", sender, ts)

        if "channel_shift_request" in names and turn <= 2:
            fire("channel_shift_early", sender, ts)

        if "contact_share" in names and priced is None:
            fire("contact_share_pre_milestone", sender, ts)

        # The gate already rejected this message on a categorical scam pattern
        # (payment manipulation / OTP request / phishing). The caller maps that
        # verdict to this signal name; it fires as an account tag directly.
        if "categorical_block" in names:
            fire("categorical_block", sender, ts)

    state = {"stages": stages, "fired": sorted(fired), "identityClaimants": sorted(claimants),
             "moneyAskers": askers, "turns": turns,
             "lastUpdated": last_ts or prev.get("lastUpdated")}
    return {"conversationId": conversation_id, "derivedTags": derived, "state": state}, {"models": []}


_REPEAT_WINDOW_DAYS = 7.0
_REPEAT_MIN_COUNTERPARTIES = 3
_RECENT_KEEP_DAYS = 30.0
_CONV_SEEN_KEEP_DAYS = 180.0   # dedupe record outlives `recent`: outcomes land well after a thread's signals


def _risk_score(tag_states: Dict[str, Dict[str, float]], tenure_days: float) -> float:
    """0-100. Weighted decayed mass through a saturating curve, discounted for
    young accounts. Outcome tags carry several times the weight of velocity tags
    by construction of FRAUD_WEIGHTS; the tenure discount is the fraud-side
    analogue of blending toward PRIOR_SCORE with PRIOR_WEIGHT virtual reviews."""
    mass = 0.0
    for tag, st in tag_states.items():
        mass += FRAUD_WEIGHTS.get(tag, 1.0) * float(st.get("w", 0.0))
    if not math.isfinite(mass):
        return 0.0          # NaN/inf in echoed state is corruption, not evidence: it must never read as 'critical'
    if mass <= 0.0:
        return 0.0          # also the case when a human clear outweighs the evidence
    tenure_factor = min(1.0, max(0.0, tenure_days) / RISK_TENURE_PRIOR_DAYS) if RISK_TENURE_PRIOR_DAYS > 0 else 1.0
    # Never discount an outcome tag: a confirmed fraud on a 1-day-old account is
    # still a confirmed fraud.
    outcome_mass = sum(FRAUD_WEIGHTS.get(t, 1.0) * float(s.get("w", 0.0))
                       for t, s in tag_states.items() if t in FRAUD_OUTCOME_SET)
    effective = outcome_mass + (mass - outcome_mass) * (0.5 + 0.5 * tenure_factor)
    score = 100.0 * (1.0 - math.exp(-effective / RISK_SCALE))
    return round(max(0.0, min(100.0, score)), 1)


def process_signals(data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Fold conversation-derived fraud tags and outcomes into per-account state.

    Input:  {profileId, state|None, tenureDays?, now?,
             signals:  [{tag, conversationId, counterpartyId, createdAt|ts}],
             outcomes: [{tag, conversationId, createdAt|ts}]}
    Output: ({profileId, riskScore 0-100, riskTier, riskTags {tag: decayed w},
              repeatedAcross [tags seen from ≥3 counterparties in 7d], state}, metrics)

    tenureDays is the account's age in days: any number is accepted (numeric strings included) and
    a negative value clamps to 0. Omit it (or send null) when unknown: the account is then scored
    with NO young-account discount (RISK_TENURE_PRIOR_DAYS). An explicit 0 means a 0-day-old account
    and IS discounted. A non-numeric or non-finite value is an input error, returned in the same
    error shape as bad signals/state. Item timestamps are read from `createdAt`,
    falling back to `ts` (the key derivedTags[] carry) when `createdAt` is absent or unparseable;
    with neither, the item counts as now and a warning is logged.

    Corrupt echoed state never raises: a fraudTagStates entry whose w/count is not a finite number
    is dropped with a warning; any other malformed container reads as empty.

    State (opaque to the caller, echoed back next call):
      fraudTagStates: {tag: {count, w}}          — once per conversation (within _CONV_SEEN_KEEP_DAYS
                                                   days of this function last seeing it), decayed
      recent:         {tag: [{cp, ts}]}          — last 30 days, for the repeat rule
      denoms:         {conversations, moneyAskConversations}
      convSeen:       {conversationId: {tags, ts}} — dedupe; ts is the processing time this function last
                                                   saw the conversation, pruned _CONV_SEEN_KEEP_DAYS later
      lastUpdated
    """
    profile_id = str(data.get("profileId") or "")
    signals = data.get("signals", []) or []
    outcomes = data.get("outcomes", []) or []
    if not isinstance(signals, list) or not isinstance(outcomes, list):
        return {"profileId": profile_id, "error": {"message": "signals/outcomes must be lists", "willRetry": False}}, {"models": []}

    now = _parse_iso(data.get("now")) or datetime.now(timezone.utc)
    # Absent/null tenure is "unknown", not "brand new": `or 0.0` would hand every such account the
    # maximum young-account discount. An explicit 0 is honoured.
    tenure_raw = data.get("tenureDays")
    try:
        tenure_days = RISK_TENURE_PRIOR_DAYS if tenure_raw is None else float(tenure_raw)
        if not math.isfinite(tenure_days):
            raise ValueError("non-finite")   # "nan"/"inf" parse as floats but are no tenure either
    except (TypeError, ValueError):
        return {"profileId": profile_id, "error": {"message": "tenureDays must be a number or null", "willRetry": False}}, {"models": []}
    # A negative tenure is nonsense; the safe direction is "brand new" (maximum discount), not an
    # error that would drop the evidence entirely.
    tenure_days = max(0.0, tenure_days)

    prev = data.get("state")
    if prev is None:
        prev = {}
    elif not isinstance(prev, dict):
        return {"profileId": profile_id, "error": {"message": "state must be an object", "willRetry": False}}, {"models": []}
    # Echoed state is caller-stored bytes: every container is shape-checked so corruption degrades
    # to "empty", never to a raise. A tag state whose w/count is not a finite number is evidence we
    # cannot trust — drop it (loudly): _risk_score would refuse the mass anyway, but the entry would
    # otherwise ride along in riskTags/state forever and json.dumps would emit Infinity/NaN.
    tag_states: Dict[str, Dict[str, float]] = {}
    raw_states = prev.get("fraudTagStates")
    for t, s in (raw_states if isinstance(raw_states, dict) else {}).items():
        try:
            w = float(s.get("w", 0.0) or 0.0) if isinstance(s, dict) else float("nan")
            count = int(s.get("count", 0) or 0) if isinstance(s, dict) else 0
        except (TypeError, ValueError, OverflowError):   # int(inf) is OverflowError
            w, count = float("nan"), 0
        if math.isfinite(w):
            tag_states[t] = {**s, "count": count, "w": w}
        else:
            logger.warning("process_signals %s: dropping corrupt fraudTagStates[%s]=%r", profile_id, t, s)
    raw_recent = prev.get("recent")
    recent: Dict[str, List[Dict[str, str]]] = {t: list(v) for t, v in (raw_recent if isinstance(raw_recent, dict) else {}).items()
                                               if isinstance(v, list)}
    conv_seen: Dict[str, Dict[str, Any]] = {}
    raw_seen = prev.get("convSeen")
    for c, v in (raw_seen if isinstance(raw_seen, dict) else {}).items():
        if isinstance(v, dict):   # anything else was never a dedupe record: ignore it, it is gone after this call
            tags = v.get("tags")
            conv_seen[c] = {"tags": list(tags) if isinstance(tags, list) else [], "ts": v.get("ts")}
    raw_denoms = prev.get("denoms")
    denoms = {}
    for k in ("conversations", "moneyAskConversations"):
        try:
            denoms[k] = int((raw_denoms.get(k) if isinstance(raw_denoms, dict) else 0) or 0)
        except (TypeError, ValueError, OverflowError):   # int(inf) is OverflowError
            denoms[k] = 0

    # 1) Decay stored mass forward to now, per family. `now` is caller-supplied, so pin it to the
    #    stored clock first: it must never run backwards, or lastUpdated and the convSeen stamps would.
    last = _parse_iso(prev.get("lastUpdated"))
    if last is not None and last > now:
        now = last
    if last is not None:
        age = max(0.0, (now - last).total_seconds() / 86400.0)
        for tag, st in tag_states.items():
            st["w"] = st.get("w", 0.0) * _fraud_recency_weight(age, tag)

    # 2) Fold new items, once per (conversation, tag).
    for item in list(signals) + list(outcomes):
        if not isinstance(item, dict):
            continue
        tag = item.get("tag")
        if tag not in FRAUD_TAGS:
            continue
        conv = str(item.get("conversationId") or "")
        # `createdAt` is the documented key; `ts` is what derivedTags[] carry. Fall back on PARSE
        # failure, not truthiness, so a garbage createdAt cannot mask a good ts — and neither key
        # silently scores a forwarded tag as brand new.
        created = _parse_iso(item.get("createdAt")) or _parse_iso(item.get("ts"))
        if created is None:
            logger.warning("process_signals %s: %s in %s has no parseable createdAt/ts, treating as now",
                           profile_id, tag, conv)
            created = now
        entry = conv_seen.setdefault(conv, {"tags": [], "ts": None})
        # Stamp with processing time, not the item's createdAt: redelivery runs on this clock, and an
        # item already older than the keep window at delivery must still get a live dedupe record.
        entry["ts"] = now.isoformat()   # `now` is pinned to ≥ stored lastUpdated above (enforced, not assumed), so no max() needed
        seen = entry["tags"]
        if tag in seen:
            continue
        if not seen:
            denoms["conversations"] = int(denoms.get("conversations", 0)) + 1
        seen.append(tag)
        if tag == "money_ask_no_milestone":
            denoms["moneyAskConversations"] = int(denoms.get("moneyAskConversations", 0)) + 1

        age = max(0.0, (now - created).total_seconds() / 86400.0)
        st = tag_states.setdefault(tag, {"count": 0, "w": 0.0})
        st["count"] = int(st.get("count", 0)) + 1
        st["w"] = float(st.get("w", 0.0)) + _fraud_recency_weight(age, tag)

        cp = item.get("counterpartyId")
        if isinstance(cp, str) and cp:
            recent.setdefault(tag, []).append({"cp": cp, "ts": created.isoformat()})

    # 3) Prune `recent` and `convSeen` to their keep windows; compute the repeat rule.
    keep_cutoff = now.timestamp() - _RECENT_KEEP_DAYS * 86400.0
    conv_seen_cutoff = now.timestamp() - _CONV_SEEN_KEEP_DAYS * 86400.0
    repeat_cutoff = now.timestamp() - _REPEAT_WINDOW_DAYS * 86400.0
    repeated: List[str] = []
    for tag, entries in list(recent.items()):
        # An entry needs a str counterparty (the fold only ever writes str; anything else is corrupt
        # and would be silently counted or raise as unhashable) AND a parseable ts. Treating a
        # missing ts as `now` would mean it never ages out and counts toward repeatedAcross forever.
        dated = [(x, _parse_iso(x.get("ts"))) for x in entries
                 if isinstance(x, dict) and isinstance(x.get("cp"), str) and x["cp"]]
        kept = [x for x, when in dated if when is not None and when.timestamp() >= keep_cutoff]
        recent[tag] = kept
        cps = {x["cp"] for x, when in dated if when is not None and when.timestamp() >= repeat_cutoff}
        if len(cps) >= _REPEAT_MIN_COUNTERPARTIES:
            repeated.append(tag)
        if not kept:
            del recent[tag]
    for conv, entry in list(conv_seen.items()):
        entry_ts = _parse_iso(entry.get("ts"))
        if entry_ts is None or entry_ts.timestamp() < conv_seen_cutoff:
            del conv_seen[conv]   # dedupe record ages out on its own, longer window; counts and w are unaffected

    # 4) Score.
    score = _risk_score(tag_states, tenure_days)
    tier = next(name for name, upper in RISK_TIERS if score < upper)
    risk_tags = {t: round(float(s.get("w", 0.0)), 4) for t, s in tag_states.items() if s.get("w", 0.0) > 1e-6}

    state = {
        "fraudTagStates": tag_states,
        "recent": recent,
        "convSeen": conv_seen,
        "denoms": denoms,
        "lastUpdated": now.isoformat(),
    }
    return {
        "profileId": profile_id,
        "riskScore": score,
        "riskTier": tier,
        "riskTags": risk_tags,
        "repeatedAcross": sorted(repeated),
        "state": state,
    }, {"models": []}


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

    # Carry through any state keys this function does not own — defence in depth,
    # so a foreign key is never erased. The two families must still be stored
    # SEPARATELY: this function and process_signals both write `lastUpdated` as
    # their decay clock, so one shared blob would decay each against the other's.
    _OWNED = ("lastUpdated", "totalReviews", "tagStates")
    passthrough = {k: v for k, v in (profile.get("state") or {}).items() if k not in _OWNED}
    new_state = {
        **passthrough,
        "lastUpdated": now.isoformat(),
        "totalReviews": total_reviews,
        "tagStates": tag_states,
    }

    return {
        "profileId": profile_id,
        "reputation": reputation,
        "reputationScore": overall_score,
        "totalReviews": total_reviews,
        "topTags": top_tags,
        "allTags": all_tags,
        "summary": summary,
        "state": new_state,
        "createdAt": now.isoformat(),
    }
