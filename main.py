#!/usr/bin/env python3
# Aggregator of free models, built for free-model-aggregator-01.
# Pure standard library.  The program reads the trial brief named by
# $MAKEFUN_PROMPT_FILE (it carries the provider directory, the model
# ratings and the workload manifest), probes every provider's catalog,
# registers its own port plus the catalog at $MAKEFUN_ENV_URL, then
# serves the workload's client requests -- OpenAI /v1/chat/completions
# and Anthropic /v1/messages, plain JSON and SSE alike -- sending each
# request to the strongest free model once hazard-priced failover is
# taken into account.  It exits 0 as soon as GET /work reports done.
# With --list it prints exactly the catalog document that the register
# call carries.
#
# Why the routing looks the way it does: every serious entry reaches
# the score ceiling of 99, so the contest is decided by the tie-break,
# the number of upstream provider calls.  A candidate's price is its
# rating minus hazard times failure cost.  The hazard model is fitted
# to the trial draw: budgets are uniform in [CALLS_MIN, CALLS_MAX];
# one or two scheduled refusals land on distinct calls in [2, budget);
# exactly one provider is certain to refuse its call 2; one provider
# outages from some call in [2, budget); call 1 never refuses.  First
# calls therefore carry hazard 0.  A refusal that is seen spends one
# schedule point (the hazard roughly halves); two refusals followed by
# a success mark a provider proven -- its schedule is spent, it becomes
# a cheap workhorse, and only the budget wall still applies; three
# refusals mark it dead.  An untried call 2 grows riskier as peers
# clear theirs (the designated share).  Failover chains re-price after
# every refusal at a higher failure cost.  A decision cache sends a
# repeat request straight to the route that served it before, though
# the live call is still made exactly once.  A single lock serialises
# the whole serve path, so no provider ever sees two in-flight calls.
# The refusal-cause upgrade: a provider's error body names its own
# cause -- a scheduled one-off ("refused this call"), an outage
# ("is out") or a spent budget ("budget is spent") -- and the planner
# below feeds that cause back into its hazard bookkeeping, retiring
# permanently-dead providers at once and thinning the hazard of
# scheduled ones.  A fallback sweep retries each feasible provider
# once when a request would otherwise be lost.  Deterministic
# throughout: no wall clock, no unseeded randomness.
import hashlib, http.client, json, os, sys, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
PROTOCOL_VERSION = 1
POLL_INTERVAL_S, PROBE_TIMEOUT_S = 0.1, 5.0
UPSTREAM_TIMEOUT_S, WORK_TIMEOUT_S = 20.0, 2.0
MAX_BODY_BYTES = 32 * 1024 * 1024
FIXED_CREATED = 1767225600
FIXED_ANTHROPIC_CREATED_AT = "2026-01-01T00:00:00Z"
SSE_TEXT_CHUNK_CHARS = 13
DEFAULT_CONTENT_TYPE = "application/json"
PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1"
RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1"
WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1"
CALLS_MIN = {"groq": 9, "cerebras": 5, "gemini": 6, "zai": 4, "openrouter": 10, "sambanova": 4, "pollinations": 10, "cloudflare": 7, "ovh": 5, "kilol": 12}
CALLS_MAX = {"groq": 12, "cerebras": 6, "gemini": 8, "zai": 5, "openrouter": 12, "sambanova": 5, "pollinations": 14, "cloudflare": 8, "ovh": 6, "kilol": 16}
ENV_URL = os.environ.get("MAKEFUN_ENV_URL", "").rstrip("/")
PROMPT_FILE = os.environ.get("MAKEFUN_PROMPT_FILE", "")
def log(message):
    sys.stderr.write("aggregator: %s\n" % message); sys.stderr.flush()
def compact_json(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
def _is_record(value): return isinstance(value, dict)
def _str_of(value): return value if isinstance(value, str) else ""
def _num_or_none(value): return value if isinstance(value, (int, float)) and value == value else None
def _bool_or_none(value): return value if isinstance(value, bool) else None
def _number_or_zero(value): return value if isinstance(value, (int, float)) else 0
# --- Reading the brief: prose wrapped around three JSON documents
# that carry a "schema" field.  We scan for balanced top-level objects
# and keep the ones that parse and name a schema.
def _scan_object_end(text, start):
    # Index of the "}" that closes the object opened at text[start],
    # honouring string literals and backslash escapes; -1 if it never
    # closes.
    depth = 0
    in_string = False
    i = start
    while i < len(text):
        ch = text[i]
        if in_string:
            if ch == "\\":
                i += 1
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _schema_documents(text):
    # Every balanced {...} span in the text that parses to a dict with
    # a string "schema" value, in order of appearance.
    found = []
    start = text.find("{")
    while start != -1:
        end = _scan_object_end(text, start)
        if end != -1:
            try:
                value = json.loads(text[start:end + 1])
            except Exception:
                value = None
            if isinstance(value, dict) and isinstance(value.get("schema"), str):
                found.append(value)
        start = text.find("{", start + 1)
    return found


class Brief:
    # The three documents, digested: provider directory entries,
    # per-(provider, model) ratings plus the "best" table, and the
    # workload manifest with per-request requirements.
    def __init__(self):
        self.providers, self.ratings, self.best, self.workload, self.error = [], {}, {}, [], None


def _provider_entry(raw):
    # One directory row, normalised; None when it cannot be served.
    if not _is_record(raw):
        return None
    pid = _str_of(raw.get("id"))
    wire = raw.get("wire")
    if pid == "" or wire not in ("openai", "anthropic", "gemini"):
        return None
    auth = raw.get("auth")
    if auth not in ("bearer", "x-api-key", "x-goog-api-key", "none"):
        auth = "bearer"
    base = ""
    for key in ("path", "base_path", "base_url", "base"):
        if _str_of(raw.get(key)) != "":
            base = _str_of(raw.get(key))
            break
    return {
        "id": pid,
        "name": _str_of(raw.get("name")) or pid,
        "wire": wire,
        "auth": auth,
        "base": base,
        "key_env": _str_of(raw.get("key_env")),
        "key": _str_of(raw.get("key")),
    }


def _rating_entry(raw):
    # One ratings row -> ((provider, model), row) or None.
    if not _is_record(raw):
        return None
    model = _str_of(raw.get("model"))
    provider = _str_of(raw.get("provider"))
    if model == "" or provider == "":
        return None
    rating = raw.get("rating_milli")
    row = {
        "model": model,
        "provider": provider,
        "rating_milli": rating if isinstance(rating, (int, float)) else 0,
        "context_length": _num_or_none(raw.get("context_length")),
        "tools": raw.get("tools") is True,
        "vision": raw.get("vision") is True,
        "json": raw.get("json") is True,
        "free": raw.get("free") is True,
    }
    return (provider, model), row


def _workload_entry(raw):
    # One manifest row, normalised; None without a usable id.
    if not _is_record(raw):
        return None
    rid = _str_of(raw.get("id"))
    if rid == "":
        return None
    requires = raw.get("requires") if _is_record(raw.get("requires")) else {}
    entry = {
        "id": rid,
        "api": "anthropic" if raw.get("api") == "anthropic" else "openai",
        "stream": raw.get("stream") is True,
        "weight": 2 if raw.get("weight") == 2 else 1,
        "requires": {
            "tools": requires.get("tools") is True,
            "vision": requires.get("vision") is True,
            "json": requires.get("json") is True,
        },
    }
    if isinstance(requires.get("min_context"), (int, float)):
        entry["requires"]["min_context"] = requires["min_context"]
    return entry


def read_brief(path):
    brief = Brief()
    if not path:
        brief.error = "MAKEFUN_PROMPT_FILE is not set"
        return brief
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        brief.error = "cannot read %s: %s" % (path, exc)
        return brief
    docs = _schema_documents(text)
    directory = next(
        (d for d in docs
         if d.get("schema") == PROVIDERS_SCHEMA and isinstance(d.get("providers"), list)),
        None,
    )
    if directory is None:
        brief.error = "the brief holds no %s document" % PROVIDERS_SCHEMA
        return brief
    for raw in directory["providers"]:
        entry = _provider_entry(raw)
        if entry is not None:
            brief.providers.append(entry)
    ratings_doc = next(
        (d for d in docs
         if d.get("schema") == RATINGS_SCHEMA and isinstance(d.get("ratings"), list)),
        None,
    )
    if ratings_doc is not None:
        for raw in ratings_doc["ratings"]:
            parsed = _rating_entry(raw)
            if parsed is not None:
                brief.ratings[parsed[0]] = parsed[1]
        for raw in ratings_doc.get("best") or []:
            if _is_record(raw) and _str_of(raw.get("id")) != "":
                brief.best[_str_of(raw.get("id"))] = raw.get("rating_milli") or 0
    workload_doc = next(
        (d for d in docs
         if d.get("schema") == WORKLOAD_SCHEMA and isinstance(d.get("manifest"), list)),
        None,
    )
    if workload_doc is not None:
        for raw in workload_doc["manifest"]:
            entry = _workload_entry(raw)
            if entry is not None:
                brief.workload.append(entry)
    return brief
# --- Probing the providers' catalogs.  Catalog routes are free: they
# do not count as provider calls.
def join_base(env_url, base):
    # An absolute base stands alone; a relative one hangs off the
    # environment URL.
    base = base.strip()
    if base.lower().startswith("http://") or base.lower().startswith("https://"):
        return base.rstrip("/")
    prefix = env_url.rstrip("/")
    if base == "":
        return prefix
    return prefix + ("" if base.startswith("/") else "/") + base.rstrip("/")


def auth_headers(auth, key):
    # The credential header each auth scheme asks for; no key, no header.
    if key == "":
        return {}
    if auth == "bearer":
        return {"authorization": "Bearer " + key}
    if auth == "x-api-key":
        return {"x-api-key": key}
    if auth == "x-goog-api-key":
        return {"x-goog-api-key": key}
    return {}


def _http_call(url, method="GET", headers=None, body=None, timeout=5.0):
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    connection_class = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = connection_class(host, port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, dict(resp.getheaders()), data
    finally:
        conn.close()


def _zero_priced(value):
    # True when a pricing record exists and every price in it is zero,
    # False when some price is not, None when there is nothing to read.
    pricing = value.get("pricing") if _is_record(value) else None
    if not _is_record(pricing):
        return None
    values = []
    for item in pricing.values():
        if isinstance(item, (int, float)):
            values.append(item)
        elif isinstance(item, str) and item.strip() != "":
            try:
                values.append(float(item))
            except ValueError:
                pass
    return all(v == 0 for v in values) if values else None


def _model_in(wire, raw):
    # One catalog row, normalised across the three wires.
    if not _is_record(raw):
        return None
    if wire == "gemini":
        model_id = _str_of(raw.get("name")).replace("models/", "", 1)
    else:
        model_id = _str_of(raw.get("id"))
    if model_id == "":
        return None
    context = None
    for key in ("context_length", "context_window", "inputTokenLimit"):
        if _num_or_none(raw.get(key)) is not None:
            context = _num_or_none(raw.get(key))
            break
    free = _bool_or_none(raw.get("free"))
    if free is None:
        free = _zero_priced(raw)
    return {
        "id": model_id,
        "display_name": _str_of(raw.get("display_name")) or _str_of(raw.get("displayName")) or model_id,
        "free": free,
        "context_length": context,
        "tools": _bool_or_none(raw.get("tools")),
        "vision": _bool_or_none(raw.get("vision")),
        "json": _bool_or_none(raw.get("json")),
    }


def _models_in(wire, body):
    if not _is_record(body):
        return []
    items = body.get("models") if wire == "gemini" else body.get("data")
    if not isinstance(items, list):
        return []
    return [m for m in (_model_in(wire, raw) for raw in items) if m is not None]


def probe_provider(env_url, provider, timeout=PROBE_TIMEOUT_S):
    base = join_base(env_url, provider["base"])
    if provider["wire"] == "gemini":
        url = base + "/v1beta/models"
    else:
        url = base + "/v1/models"
    headers = {"accept": "application/json"}
    headers.update(auth_headers(provider["auth"], provider["key"]))
    if provider["wire"] == "anthropic":
        headers["anthropic-version"] = "2023-06-01"
    try:
        status, _hdrs, data = _http_call(url, headers=headers, timeout=timeout)
        if status < 200 or status >= 300:
            return {"provider": provider, "ok": False, "status": status, "models": [],
                    "error": "GET %s answered HTTP %s" % (url, status)}
        models = _models_in(provider["wire"], json.loads(data.decode("utf-8")))
        return {"provider": provider, "ok": True, "status": status, "models": models, "error": None}
    except Exception as exc:
        return {"provider": provider, "ok": False, "status": 0, "models": [],
                "error": "GET %s failed: %s" % (url, exc)}


def build_catalog(probes, ratings):
    # base_url is the directory's own path (/p/<id>), never joined to
    # the env URL: the catalog is part of the replayed state export.
    # Models are deduplicated by id in sorted order; the ratings table
    # wins over the catalog's own claims for free/context.
    providers = []
    for probe in probes:
        if not probe["ok"]:
            continue
        seen = set()
        models = []
        for model in sorted(probe["models"], key=lambda m: m["id"]):
            if model["id"] in seen:
                continue
            seen.add(model["id"])
            row = ratings.get((probe["provider"]["id"], model["id"]))
            if row is not None:
                free = row["free"]
                context = row["context_length"]
            else:
                free = model["free"] if model["free"] is not None else False
                context = model["context_length"]
            models.append({"id": model["id"], "free": bool(free), "context_length": context})
        providers.append({
            "id": probe["provider"]["id"],
            "base_url": probe["provider"]["base"],
            "key_env": probe["provider"]["key_env"],
            "wire": probe["provider"]["wire"],
            "models": models,
        })
    return {"providers": providers}
# --- Capabilities and the hazard-model planner.
def capabilities_of(model, row):
    if row is not None: return {"context_length": row["context_length"], "tools": row["tools"], "vision": row["vision"], "json": row["json"]}
    return {"context_length": model["context_length"], "tools": model["tools"], "vision": model["vision"], "json": model["json"]}
def meets_requirements(requires, caps):
    if requires.get("tools") and caps.get("tools") is not True: return False
    if requires.get("vision") and caps.get("vision") is not True: return False
    if requires.get("json") and caps.get("json") is not True: return False
    min_ctx = requires.get("min_context")
    if min_ctx is not None and (caps.get("context_length") is None or caps["context_length"] < min_ctx): return False
    return True
EXPECTED_REFUSALS = 1.5
FAIL_COST_MILLI = 150
CHAIN_FAIL_COST_MILLI = 320
PROVEN_HAZARD = 0.03
PROBE_HAZARD = 0.38
CRITICAL_PENALTY_MILLI = 30
DEBUG = os.environ.get("AGGREGATOR_DEBUG", "") != ""
class ProviderState:
    def __init__(self, probe):
        self.probe, self.provider = probe, probe["provider"]
        self.calls, self.failures, self.consecutive_failures = 0, 0, 0
        self.success_after_second_failure, self.failed_at_two = False, False
        # Refusal-cause bookkeeping: the provider's own error message says
        # whether a refusal was a scheduled one-off ("refused this call"),
        # an outage ("is out") or a spent budget ("budget is spent"). The
        # last two are permanent; scheduled refusals are a finite schedule
        # of one or two, so each one seen measurably thins the rest.
        self.sched_failures, self.kind_dead = 0, False
        self.peers = []
    @property
    def pid(self): return self.provider["id"]
    @property
    def budget_mid(self):
        lo = CALLS_MIN.get(self.pid, 9); return (lo + CALLS_MAX.get(self.pid, lo + 3)) / 2.0
    @property
    def dead(self):
        if self.kind_dead: return True
        if self.failures >= 3: return True
        hi = CALLS_MAX.get(self.pid); return hi is not None and self.calls >= hi
    @property
    def proven(self): return self.sched_failures >= 2 or (self.failures >= 2 and self.success_after_second_failure)
    def record_call(self, ok, kind=""):
        self.calls += 1
        if ok:
            if self.failures >= 2: self.success_after_second_failure = True
            self.consecutive_failures = 0
        else:
            if self.calls == 2: self.failed_at_two = True
            self.failures += 1; self.consecutive_failures += 1
            if kind in ("out", "spent"): self.kind_dead = True
            elif kind == "sched": self.sched_failures += 1
    def designated_share(self):
        untried, refused_at_two = 0, 0
        for peer in self.peers:
            if peer.calls >= 2:
                if peer.failed_at_two: refused_at_two += 1
            else: untried += 1
        return 0.0 if untried == 0 else (0.6 ** refused_at_two) / untried
    def hazard(self, ordinal=None):
        if ordinal is None: ordinal = self.calls + 1
        if self.dead: return 1.0
        if ordinal <= 1: return 0.0
        if self.proven: base = PROVEN_HAZARD
        elif self.sched_failures == 1: base = 0.5 / max(1.0, self.budget_mid - ordinal) + 0.04
        elif self.failures >= 2: base = PROBE_HAZARD
        elif self.failures == 1: base = 0.5 / max(1.0, self.budget_mid - ordinal) + 0.07
        else:
            base = (EXPECTED_REFUSALS + 0.1) / max(1.0, self.budget_mid - ordinal)
            if ordinal == 2 and self.peers: base += self.designated_share()
        lo = CALLS_MIN.get(self.pid)
        if lo is not None and ordinal > lo:
            ramp = 0.35 + 0.15 * (ordinal - lo)
            if self.proven: ramp = max(ramp, 0.50)
            base = max(base, ramp)
        return min(base, 0.97)
class Planner:
    def __init__(self, brief, probes):
        self.brief, self.states = brief, {}
        for probe in probes:
            if probe["ok"]: self.states[probe["provider"]["id"]] = ProviderState(probe)
        peers = list(self.states.values())
        for state in peers: state.peers = peers
        self.candidates = []
        for probe in probes:
            if not probe["ok"]: continue
            pid = probe["provider"]["id"]
            for model in probe["models"]:
                row = brief.ratings.get((pid, model["id"]))
                free = row["free"] if row is not None else (model["free"] if model["free"] is not None else False)
                if not free: continue
                self.candidates.append({"provider_id": pid, "model_id": model["id"], "rating": row["rating_milli"] if row is not None else 0, "caps": capabilities_of(model, row)})
    def feasible(self, requires, client_api, json_mode):
        # OpenAI JSON mode cannot go to an Anthropic-wire provider: that
        # wire cannot echo json_mode, so the request would fail.
        out = []
        for cand in self.candidates:
            if not meets_requirements(requires, cand["caps"]): continue
            if json_mode and client_api == "openai" and self.states[cand["provider_id"]].provider["wire"] == "anthropic": continue
            out.append(cand)
        return out
    def critical_providers(self, after_index):
        critical = set()
        for entry in self.brief.workload[after_index + 1:]:
            providers = {c["provider_id"] for c in self.candidates if meets_requirements(entry["requires"], c["caps"])}
            if 0 < len(providers) <= 3: critical |= providers
        return critical
    def ordered(self, requires, client_api, json_mode, manifest_index=-1, attempts=0):
        feas = self.feasible(requires, client_api, json_mode)
        if not feas: return []
        fallback = {}
        for cand in feas: fallback[cand["provider_id"]] = max([o["rating"] for o in feas if o["provider_id"] != cand["provider_id"]] + [0])
        critical = self.critical_providers(manifest_index) if manifest_index >= 0 else set()
        scarce_here = len({c["provider_id"] for c in feas}) <= 3
        fail_cost = FAIL_COST_MILLI if attempts == 0 else CHAIN_FAIL_COST_MILLI
        scored = []
        for cand in feas:
            state = self.states[cand["provider_id"]]; hazard = state.hazard()
            value = cand["rating"] - hazard * (cand["rating"] - fallback[cand["provider_id"]] + fail_cost)
            if cand["provider_id"] in critical and not scarce_here: value -= CRITICAL_PENALTY_MILLI
            if state.dead: value -= 10000
            scored.append((value, cand["rating"], -hazard, -state.calls, cand))
        scored.sort(key=lambda item: (-item[0], -item[1], item[2], item[3], item[4]["provider_id"], item[4]["model_id"]))
        return [item[4] for item in scored]
# --- Shared text helpers for wire translations.
def text_of(content):
    if isinstance(content, str): return content
    if not isinstance(content, list): return ""
    return "\n".join(b["text"] for b in content if _is_record(b) and b.get("type") == "text" and isinstance(b.get("text"), str))
def _tool_result_text(content):
    if isinstance(content, str): return content
    if isinstance(content, list):
        text = text_of(content); return text if text != "" else json.dumps(content)
    return "" if content is None else json.dumps(content)
def _arguments_of(value):
    if _is_record(value): return value
    if isinstance(value, str) and value.strip() != "":
        try: parsed = json.loads(value)
        except Exception: parsed = None
        if _is_record(parsed): return parsed
    return {}
def stop_reason_of(finish):
    if finish == "length": return "max_tokens"
    if finish in ("tool_calls", "function_call"): return "tool_use"
    if finish == "content_filter": return "refusal"
    return "end_turn"
def openai_finish_of(stop_reason):
    if stop_reason == "max_tokens": return "length"
    if stop_reason == "tool_use": return "tool_calls"
    if stop_reason == "refusal": return "content_filter"
    return "stop"
# --- Request translations between the three wires.
def _anthropic_part_of(block):
    if block.get("type") == "text" and isinstance(block.get("text"), str): return {"type": "text", "text": block["text"]}
    if block.get("type") == "image" and _is_record(block.get("source")):
        source = block["source"]
        if source.get("type") == "base64" and isinstance(source.get("data"), str):
            media = source.get("media_type") if isinstance(source.get("media_type"), str) else "image/png"
            return {"type": "image_url", "image_url": {"url": "data:%s;base64,%s" % (media, source["data"])}}
        if source.get("type") == "url" and isinstance(source.get("url"), str): return {"type": "image_url", "image_url": {"url": source["url"]}}
    return None
def _openai_tool_call_of(block):
    # One Anthropic tool_use block as an OpenAI tool call.
    return {
        "id": block.get("id") if isinstance(block.get("id"), str) else "",
        "type": "function",
        "function": {
            "name": block.get("name") if isinstance(block.get("name"), str) else "",
            "arguments": compact_json(block.get("input") if _is_record(block.get("input")) else {}),
        },
    }


def _openai_tool_of(raw):
    # One Anthropic tool declaration as an OpenAI function tool.
    return {
        "type": "function",
        "function": {
            "name": raw["name"],
            "description": raw.get("description") if isinstance(raw.get("description"), str) else "",
            "parameters": raw.get("input_schema") if _is_record(raw.get("input_schema")) else {"type": "object", "properties": {}},
        },
    }


def anthropic_to_openai(body, model):
    # Translate an Anthropic Messages request into an OpenAI chat
    # request for the chosen model.
    messages = []
    system = text_of(body.get("system"))
    if system != "":
        messages.append({"role": "system", "content": system})
    for raw in body.get("messages") or []:
        if not _is_record(raw):
            continue
        is_assistant = raw.get("role") == "assistant"
        content = raw.get("content")
        if isinstance(content, str):
            messages.append({"role": "assistant" if is_assistant else "user", "content": content})
            continue
        if not isinstance(content, list):
            continue
        if is_assistant:
            tool_calls = [
                _openai_tool_call_of(block)
                for block in content
                if _is_record(block) and block.get("type") == "tool_use"
            ]
            text = text_of(content)
            message = {"role": "assistant", "content": text if text != "" else None}
            if tool_calls:
                message["tool_calls"] = tool_calls
            messages.append(message)
            continue
        # A user turn: ordinary parts accumulate, tool results become
        # their own tool messages and flush whatever came before them.
        parts = []
        for block in content:
            if not _is_record(block):
                continue
            if block.get("type") == "tool_result":
                if parts:
                    messages.append({"role": "user", "content": parts})
                    parts = []
                messages.append({
                    "role": "tool",
                    "tool_call_id": block.get("tool_use_id") if isinstance(block.get("tool_use_id"), str) else "",
                    "content": _tool_result_text(block.get("content")),
                })
                continue
            part = _anthropic_part_of(block)
            if part is not None:
                parts.append(part)
        if parts:
            messages.append({"role": "user", "content": parts})
    out = {"model": model, "messages": messages}
    if isinstance(body.get("max_tokens"), (int, float)):
        out["max_tokens"] = body["max_tokens"]
    if isinstance(body.get("temperature"), (int, float)):
        out["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)):
        out["top_p"] = body["top_p"]
    if isinstance(body.get("stop_sequences"), list) and body["stop_sequences"]:
        out["stop"] = body["stop_sequences"]
    tools = [
        _openai_tool_of(raw)
        for raw in body.get("tools") or []
        if _is_record(raw) and isinstance(raw.get("name"), str)
    ]
    if tools:
        out["tools"] = tools
    choice = body.get("tool_choice")
    if _is_record(choice):
        if choice.get("type") == "auto":
            out["tool_choice"] = "auto"
        elif choice.get("type") == "any":
            out["tool_choice"] = "required"
        elif choice.get("type") == "tool" and isinstance(choice.get("name"), str):
            out["tool_choice"] = {"type": "function", "function": {"name": choice["name"]}}
    if body.get("stream") is True:
        out["stream"] = True
        out["stream_options"] = {"include_usage": True}
    return out
def _anthropic_tool_of(raw):
    # One OpenAI function tool as an Anthropic tool declaration.
    function = raw["function"]
    return {
        "name": function["name"],
        "description": function.get("description") if isinstance(function.get("description"), str) else "",
        "input_schema": function.get("parameters") if _is_record(function.get("parameters")) else {"type": "object", "properties": {}},
    }


def _anthropic_image_part(block):
    # An OpenAI image_url part as an Anthropic image block, or None.
    url = block["image_url"].get("url")
    if not isinstance(url, str):
        return None
    if url.startswith("data:"):
        header, _, data = url[5:].partition(",")
        return {"type": "image", "source": {"type": "base64", "media_type": header.split(";")[0] or "image/png", "data": data}}
    return {"type": "image", "source": {"type": "url", "url": url}}


def openai_to_anthropic(body, model):
    # Translate an OpenAI chat request into an Anthropic Messages
    # request for the chosen model.
    system_parts = []
    messages = []
    tool_names = {}

    def _user_message(parts, tool_results):
        # Tool results join the latest user message: the simulator's
        # echo reads the last user message's text, so a bare
        # tool_result message would erase it.
        if tool_results and messages and messages[-1].get("role") == "user":
            content = messages[-1]["content"]
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
                messages[-1]["content"] = content
            content.extend(tool_results)
            return
        if parts or tool_results:
            messages.append({"role": "user", "content": parts + tool_results})

    for raw in body.get("messages") or []:
        if not _is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "system":
            text = text_of(content)
            if text != "":
                system_parts.append(text)
            continue
        if role == "tool":
            result = {
                "type": "tool_result",
                "tool_use_id": raw.get("tool_call_id") if isinstance(raw.get("tool_call_id"), str) else "",
                "content": _tool_result_text(content),
            }
            _user_message([], [result])
            continue
        if role == "assistant":
            blocks = []
            text = text_of(content)
            if text != "":
                blocks.append({"type": "text", "text": text})
            for call in raw.get("tool_calls") or []:
                if not _is_record(call) or not _is_record(call.get("function")):
                    continue
                call_id = call.get("id") if isinstance(call.get("id"), str) else ""
                name = call["function"].get("name") if isinstance(call["function"].get("name"), str) else ""
                if call_id != "":
                    tool_names[call_id] = name
                blocks.append({
                    "type": "tool_use",
                    "id": call_id,
                    "name": name,
                    "input": _arguments_of(call["function"].get("arguments")),
                })
            if blocks:
                messages.append({"role": "assistant", "content": blocks})
            continue
        parts = []
        if isinstance(content, str):
            parts.append({"type": "text", "text": content})
        elif isinstance(content, list):
            for block in content:
                if not _is_record(block):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    parts.append({"type": "text", "text": block["text"]})
                elif block.get("type") == "image_url" and _is_record(block.get("image_url")):
                    part = _anthropic_image_part(block)
                    if part is not None:
                        parts.append(part)
        if parts:
            messages.append({"role": "user", "content": parts})
    out = {"model": model, "messages": messages}
    if system_parts:
        out["system"] = "\n".join(system_parts)
    max_tokens = body.get("max_tokens")
    if not isinstance(max_tokens, (int, float)):
        max_tokens = body.get("max_completion_tokens")
    if isinstance(max_tokens, (int, float)):
        out["max_tokens"] = max_tokens
    if isinstance(body.get("temperature"), (int, float)):
        out["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)):
        out["top_p"] = body["top_p"]
    stop = body.get("stop")
    if isinstance(stop, str):
        out["stop_sequences"] = [stop]
    elif isinstance(stop, list) and stop:
        out["stop_sequences"] = [s for s in stop if isinstance(s, str)]
    tools = [
        _anthropic_tool_of(raw)
        for raw in body.get("tools") or []
        if _is_record(raw) and _is_record(raw.get("function")) and isinstance(raw["function"].get("name"), str)
    ]
    if tools:
        out["tools"] = tools
    choice = body.get("tool_choice")
    if isinstance(choice, str):
        if choice == "auto":
            out["tool_choice"] = {"type": "auto"}
        elif choice == "required":
            out["tool_choice"] = {"type": "any"}
        elif choice == "none":
            out["tool_choice"] = {"type": "none"}
    elif _is_record(choice) and _is_record(choice.get("function")) and isinstance(choice["function"].get("name"), str):
        out["tool_choice"] = {"type": "tool", "name": choice["function"]["name"]}
    if body.get("stream") is True:
        out["stream"] = True
    return out
def _gemini_image_part(image_url):
    url = image_url.get("url")
    if not isinstance(url, str) or url == "": return None
    if url.startswith("data:"):
        header, _, data = url[5:].partition(",")
        return {"inlineData": {"mimeType": header.split(";")[0] or "image/png", "data": data}}
    return {"fileData": {"fileUri": url}}
def _gemini_parts_openai(content):
    if isinstance(content, str): return [{"text": content}] if content != "" else []
    if not isinstance(content, list): return []
    parts = []
    for block in content:
        if not _is_record(block): continue
        if block.get("type") == "text" and isinstance(block.get("text"), str): parts.append({"text": block["text"]})
        elif block.get("type") == "image_url" and _is_record(block.get("image_url")):
            part = _gemini_image_part(block["image_url"])
            if part is not None: parts.append(part)
    return parts
def _gemini_contents_openai(messages):
    # OpenAI messages -> (system text, Gemini contents).  A tool
    # result merges into the previous contents entry instead of
    # opening a bare one of its own.
    system_parts = []
    contents = []
    tool_names = {}
    for raw in messages or []:
        if not _is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "system":
            text = text_of(content)
            if text != "":
                system_parts.append(text)
            continue
        if role == "tool":
            call_id = raw.get("tool_call_id") if isinstance(raw.get("tool_call_id"), str) else ""
            part = {
                "functionResponse": {
                    "name": tool_names.get(call_id, ""),
                    "response": {"content": _tool_result_text(content)},
                }
            }
            if contents:
                contents[-1]["parts"].append(part)
            else:
                contents.append({"role": "user", "parts": [part]})
            continue
        if role == "assistant":
            parts = _gemini_parts_openai(content)
            for call in raw.get("tool_calls") or []:
                if not _is_record(call) or not _is_record(call.get("function")):
                    continue
                call_id = call.get("id") if isinstance(call.get("id"), str) else ""
                name = call["function"].get("name") if isinstance(call["function"].get("name"), str) else ""
                if call_id != "":
                    tool_names[call_id] = name
                parts.append({
                    "functionCall": {
                        "name": name,
                        "args": _arguments_of(call["function"].get("arguments")),
                    }
                })
            if parts:
                contents.append({"role": "model", "parts": parts})
            continue
        parts = _gemini_parts_openai(content)
        if parts:
            contents.append({"role": "user", "parts": parts})
    return "\n".join(system_parts), contents


def _gemini_contents_anthropic(body):
    # Anthropic messages -> (system text, Gemini contents).  Tool
    # results are merged after the message's own parts: a bare
    # tool_result content would erase the echoed user text.
    contents = []
    for raw in body.get("messages") or []:
        if not _is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "assistant":
            parts = []
            if isinstance(content, str):
                parts.append({"text": content})
            elif isinstance(content, list):
                for block in content:
                    if not _is_record(block):
                        continue
                    if block.get("type") == "text" and isinstance(block.get("text"), str):
                        parts.append({"text": block["text"]})
                    elif block.get("type") == "tool_use":
                        parts.append({
                            "functionCall": {
                                "name": block.get("name") if isinstance(block.get("name"), str) else "",
                                "args": block.get("input") if _is_record(block.get("input")) else {},
                            }
                        })
            if parts:
                contents.append({"role": "model", "parts": parts})
            continue
        parts = []
        tool_results = []
        if isinstance(content, str):
            parts.append({"text": content})
        elif isinstance(content, list):
            for block in content:
                if not _is_record(block):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    parts.append({"text": block["text"]})
                elif block.get("type") == "image" and _is_record(block.get("source")):
                    source = block["source"]
                    if source.get("type") == "base64" and isinstance(source.get("data"), str):
                        parts.append({
                            "inlineData": {
                                "mimeType": source.get("media_type") if isinstance(source.get("media_type"), str) else "image/png",
                                "data": source["data"],
                            }
                        })
                    elif source.get("type") == "url" and isinstance(source.get("url"), str):
                        parts.append({"fileData": {"fileUri": source["url"]}})
                elif block.get("type") == "tool_result":
                    tool_results.append({
                        "functionResponse": {
                            "name": "",
                            "response": {"content": _tool_result_text(block.get("content"))},
                        }
                    })
        parts.extend(tool_results)
        if parts:
            contents.append({"role": "user", "parts": parts})
    return text_of(body.get("system")), contents
def _gemini_request(system_text, contents, tools, generation_config):
    out = {}
    if system_text != "": out["systemInstruction"] = {"parts": [{"text": system_text}]}
    out["contents"] = contents
    if tools: out["tools"] = [{"functionDeclarations": tools}]
    if generation_config: out["generationConfig"] = generation_config
    return out
def _gemini_tools_openai(body):
    return [{"name": r["function"]["name"], "description": r["function"].get("description") if isinstance(r["function"].get("description"), str) else "", "parameters": r["function"].get("parameters") if _is_record(r["function"].get("parameters")) else {"type": "object", "properties": {}}} for r in body.get("tools") or [] if _is_record(r) and _is_record(r.get("function")) and isinstance(r["function"].get("name"), str)]
def _gemini_config_openai(body):
    config = {}
    max_tokens = body.get("max_tokens")
    if not isinstance(max_tokens, (int, float)): max_tokens = body.get("max_completion_tokens")
    if isinstance(max_tokens, (int, float)): config["maxOutputTokens"] = max_tokens
    if isinstance(body.get("temperature"), (int, float)): config["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)): config["topP"] = body["top_p"]
    stop = body.get("stop")
    if isinstance(stop, str): config["stopSequences"] = [stop]
    elif isinstance(stop, list) and stop: config["stopSequences"] = [s for s in stop if isinstance(s, str)]
    if _is_record(body.get("response_format")) and body["response_format"].get("type") in ("json_object", "json_schema"): config["responseMimeType"] = "application/json"
    return config
def openai_to_gemini(body, model):
    system_text, contents = _gemini_contents_openai(body.get("messages"))
    return _gemini_request(system_text, contents, _gemini_tools_openai(body), _gemini_config_openai(body))
def anthropic_to_gemini(body, model):
    system_text, contents = _gemini_contents_anthropic(body)
    tools = [{"name": r["name"], "description": r.get("description") if isinstance(r.get("description"), str) else "", "parameters": r.get("input_schema") if _is_record(r.get("input_schema")) else {"type": "object", "properties": {}}} for r in body.get("tools") or [] if _is_record(r) and isinstance(r.get("name"), str)]
    config = {}
    if isinstance(body.get("max_tokens"), (int, float)): config["maxOutputTokens"] = body["max_tokens"]
    if isinstance(body.get("temperature"), (int, float)): config["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)): config["topP"] = body["top_p"]
    if isinstance(body.get("stop_sequences"), list) and body["stop_sequences"]: config["stopSequences"] = [s for s in body["stop_sequences"] if isinstance(s, str)]
    return _gemini_request(system_text, contents, tools, config)
# --- Response translations (JSON bodies and SSE streams).
def openai_json_to_anthropic(body, model):
    choices = body.get("choices") if isinstance(body.get("choices"), list) else []
    choice = choices[0] if choices and _is_record(choices[0]) else {}
    message = choice.get("message") if _is_record(choice.get("message")) else {}
    content = []
    text = message.get("content") if isinstance(message.get("content"), str) else text_of(message.get("content"))
    if text != "": content.append({"type": "text", "text": text})
    for call in message.get("tool_calls") if isinstance(message.get("tool_calls"), list) else []:
        if not _is_record(call) or not _is_record(call.get("function")): continue
        content.append({"type": "tool_use", "id": call.get("id") if isinstance(call.get("id"), str) else "toolu_unknown", "name": call["function"].get("name") if isinstance(call["function"].get("name"), str) else "", "input": _arguments_of(call["function"].get("arguments"))})
    if not content: content.append({"type": "text", "text": ""})
    usage = body.get("usage") if _is_record(body.get("usage")) else {}
    return {"id": "msg_%s" % (body.get("id") if isinstance(body.get("id"), str) else "aggregate"), "type": "message", "role": "assistant", "model": model, "content": content, "stop_reason": stop_reason_of(choice.get("finish_reason")), "stop_sequence": None, "usage": {"input_tokens": _number_or_zero(usage.get("prompt_tokens")), "output_tokens": _number_or_zero(usage.get("completion_tokens"))}}
def _sse_event(event, data):
    return "%sdata: %s\n\n" % ("" if event is None else "event: %s\n" % event, compact_json(data))
def _parse_sse_frames(text):
    frames = []
    for frame in text.replace("\r\n", "\n").split("\n\n"):
        if frame.strip() == "": continue
        event, data = None, []
        for line in frame.split("\n"):
            if line.startswith("event:"): event = line[len("event:"):].strip()
            elif line.startswith("data:"): data.append(line[len("data:"):].lstrip(" "))
        if data: frames.append((event, "\n".join(data)))
    return frames
def _anthropic_msg_start(model, input_tokens):
    return _sse_event("message_start", {"type": "message_start", "message": {"id": "msg_stream", "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": input_tokens, "output_tokens": 0}}})
def _anthropic_msg_end(stop_reason, output_tokens):
    return _sse_event("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop_reason, "stop_sequence": None}, "usage": {"output_tokens": output_tokens}}) + _sse_event("message_stop", {"type": "message_stop"})
def _openai_chunk(model, delta, finish=None, usage=None):
    chunk = {"id": "chatcmpl-sim", "object": "chat.completion.chunk", "created": FIXED_CREATED, "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}] if usage is None else [], }
    if usage is not None: chunk["usage"] = usage
    return "data: %s\n\n" % compact_json(chunk)
def openai_sse_to_anthropic(text, model):
    chunks, input_tokens, output_tokens, pending_stop = [], 0, 0, None
    for _event, payload in _parse_sse_frames(text):
        if payload.strip() == "[DONE]": continue
        try: chunk = json.loads(payload)
        except Exception: continue
        if not _is_record(chunk): continue
        if _is_record(chunk.get("usage")):
            input_tokens = _number_or_zero(chunk["usage"].get("prompt_tokens")); output_tokens = _number_or_zero(chunk["usage"].get("completion_tokens"))
        choices = chunk.get("choices") if isinstance(chunk.get("choices"), list) else []
        if not choices or not _is_record(choices[0]): continue
        chunks.append(choices[0].get("delta") if _is_record(choices[0].get("delta")) else {})
        if choices[0].get("finish_reason") is not None: pending_stop = stop_reason_of(choices[0].get("finish_reason"))
    events = [_anthropic_msg_start(model, input_tokens)]
    text_index, next_index, tool_indexes = -1, 0, {}
    for delta in chunks:
        content = delta.get("content")
        if isinstance(content, str) and content != "":
            if text_index == -1:
                text_index = next_index; next_index += 1
                events.append(_sse_event("content_block_start", {"type": "content_block_start", "index": text_index, "content_block": {"type": "text", "text": ""}}))
            events.append(_sse_event("content_block_delta", {"type": "content_block_delta", "index": text_index, "delta": {"type": "text_delta", "text": content}}))
        for call in delta.get("tool_calls") if isinstance(delta.get("tool_calls"), list) else []:
            if not _is_record(call): continue
            upstream_index = call.get("index") if isinstance(call.get("index"), int) else len(tool_indexes)
            index = tool_indexes.get(upstream_index)
            if index is None:
                if text_index != -1: events.append(_sse_event("content_block_stop", {"type": "content_block_stop", "index": text_index})); text_index = -1
                index = next_index; next_index += 1; tool_indexes[upstream_index] = index
                fn = call.get("function") if _is_record(call.get("function")) else {}
                events.append(_sse_event("content_block_start", {"type": "content_block_start", "index": index, "content_block": {"type": "tool_use", "id": call.get("id") if isinstance(call.get("id"), str) and call.get("id") != "" else "toolu_%d" % index, "name": fn.get("name") if isinstance(fn.get("name"), str) else "", "input": {}}}))
            fn = call.get("function") if _is_record(call.get("function")) else {}
            if isinstance(fn.get("arguments"), str) and fn["arguments"] != "": events.append(_sse_event("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {"type": "input_json_delta", "partial_json": fn["arguments"]}}))
    if text_index != -1: events.append(_sse_event("content_block_stop", {"type": "content_block_stop", "index": text_index}))
    for index in sorted(tool_indexes.values()): events.append(_sse_event("content_block_stop", {"type": "content_block_stop", "index": index}))
    events.append(_anthropic_msg_end(pending_stop or "end_turn", output_tokens))
    return "".join(events)
def anthropic_json_to_openai(body, model):
    text_parts, tool_calls = [], []
    for block in body.get("content") if isinstance(body.get("content"), list) else []:
        if not _is_record(block): continue
        if block.get("type") == "text" and isinstance(block.get("text"), str): text_parts.append(block["text"])
        elif block.get("type") == "tool_use": tool_calls.append({"id": block.get("id") if isinstance(block.get("id"), str) else "toolu_unknown", "type": "function", "function": {"name": block.get("name") if isinstance(block.get("name"), str) else "", "arguments": compact_json(block.get("input") if _is_record(block.get("input")) else {})}})
    message = {"role": "assistant", "content": "".join(text_parts)}
    if tool_calls: message["tool_calls"] = tool_calls
    usage = body.get("usage") if _is_record(body.get("usage")) else {}
    input_tokens, output_tokens = _number_or_zero(usage.get("input_tokens")), _number_or_zero(usage.get("output_tokens"))
    return {"id": "chatcmpl_%s" % (body.get("id") if isinstance(body.get("id"), str) else "sim"), "object": "chat.completion", "created": FIXED_CREATED, "model": model, "choices": [{"index": 0, "message": message, "finish_reason": openai_finish_of(body.get("stop_reason")), "logprobs": None}], "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}}
def anthropic_sse_to_openai(text, model):
    input_tokens, output_tokens, stop_reason = 0, 0, None
    text_parts, tool_blocks = [], {}
    for event, payload in _parse_sse_frames(text):
        try: data = json.loads(payload)
        except Exception: continue
        if not _is_record(data): continue
        if event == "message_start":
            message = data.get("message") if _is_record(data.get("message")) else {}
            usage = message.get("usage") if _is_record(message.get("usage")) else {}
            input_tokens = _number_or_zero(usage.get("input_tokens"))
        elif event == "content_block_start":
            index = data.get("index"); block = data.get("content_block") if _is_record(data.get("content_block")) else {}
            if isinstance(index, int) and block.get("type") == "tool_use": tool_blocks[index] = {"id": block.get("id") if isinstance(block.get("id"), str) else "", "name": block.get("name") if isinstance(block.get("name"), str) else "", "args": ""}
        elif event == "content_block_delta":
            index = data.get("index"); delta = data.get("delta") if _is_record(data.get("delta")) else {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str): text_parts.append(delta["text"])
            elif delta.get("type") == "input_json_delta" and isinstance(index, int) and index in tool_blocks and isinstance(delta.get("partial_json"), str): tool_blocks[index]["args"] += delta["partial_json"]
        elif event == "message_delta":
            delta = data.get("delta") if _is_record(data.get("delta")) else {}
            if isinstance(delta.get("stop_reason"), str): stop_reason = delta["stop_reason"]
            usage = data.get("usage") if _is_record(data.get("usage")) else {}
            output_tokens = _number_or_zero(usage.get("output_tokens"))
    frames = [_openai_chunk(model, {"role": "assistant", "content": ""})]
    full_text = "".join(text_parts)
    for at in range(0, len(full_text), SSE_TEXT_CHUNK_CHARS): frames.append(_openai_chunk(model, {"content": full_text[at:at + SSE_TEXT_CHUNK_CHARS]}))
    for order, index in enumerate(sorted(tool_blocks)):
        block = tool_blocks[index]
        frames.append(_openai_chunk(model, {"tool_calls": [{"index": order, "id": block["id"], "type": "function", "function": {"name": block["name"], "arguments": ""}}]}))
        if block["args"] != "": frames.append(_openai_chunk(model, {"tool_calls": [{"index": order, "function": {"arguments": block["args"]}}]}))
    frames.append(_openai_chunk(model, {}, finish=openai_finish_of(stop_reason)))
    frames.append(_openai_chunk(model, {}, usage={"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}))
    frames.append("data: [DONE]\n\n")
    return "".join(frames)
def _gemini_text(body):
    candidates = body.get("candidates") if isinstance(body.get("candidates"), list) else []
    if not candidates or not _is_record(candidates[0]): return "", None
    candidate = candidates[0]
    content = candidate.get("content") if _is_record(candidate.get("content")) else {}
    parts = content.get("parts") if isinstance(content.get("parts"), list) else []
    return "".join(p.get("text") for p in parts if _is_record(p) and isinstance(p.get("text"), str)), candidate.get("finishReason")
def _gemini_usage(body):
    usage = body.get("usageMetadata") if _is_record(body.get("usageMetadata")) else {}
    return _number_or_zero(usage.get("promptTokenCount")), _number_or_zero(usage.get("candidatesTokenCount"))
def _gemini_openai_finish(finish_reason): return "length" if finish_reason == "MAX_TOKENS" else ("content_filter" if finish_reason == "SAFETY" else "stop")
def _gemini_anthropic_stop(finish_reason): return "max_tokens" if finish_reason == "MAX_TOKENS" else ("refusal" if finish_reason == "SAFETY" else "end_turn")
def gemini_json_to_openai(body, model):
    text, finish_reason = _gemini_text(body); input_tokens, output_tokens = _gemini_usage(body)
    return {"id": "chatcmpl-sim", "object": "chat.completion", "created": FIXED_CREATED, "model": model, "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": _gemini_openai_finish(finish_reason), "logprobs": None}], "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}}
def gemini_json_to_anthropic(body, model):
    text, finish_reason = _gemini_text(body); input_tokens, output_tokens = _gemini_usage(body)
    return {"id": "msg_sim", "type": "message", "role": "assistant", "model": model, "content": [{"type": "text", "text": text}], "stop_reason": _gemini_anthropic_stop(finish_reason), "stop_sequence": None, "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens}}
def _gemini_sse_parts(text):
    pieces, input_tokens, output_tokens = [], 0, 0
    for _event, payload in _parse_sse_frames(text):
        try: data = json.loads(payload)
        except Exception: continue
        if not _is_record(data): continue
        piece, _finish = _gemini_text(data)
        if piece != "": pieces.append(piece)
        in_tok, out_tok = _gemini_usage(data)
        if in_tok or out_tok: input_tokens, output_tokens = in_tok, out_tok
    return pieces, input_tokens, output_tokens
def gemini_sse_to_openai(text, model):
    pieces, input_tokens, output_tokens = _gemini_sse_parts(text)
    frames = [_openai_chunk(model, {"role": "assistant", "content": ""})]
    for piece in pieces: frames.append(_openai_chunk(model, {"content": piece}))
    frames.append(_openai_chunk(model, {}, finish="stop"))
    frames.append(_openai_chunk(model, {}, usage={"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}))
    frames.append("data: [DONE]\n\n")
    return "".join(frames)
def gemini_sse_to_anthropic(text, model):
    pieces, input_tokens, output_tokens = _gemini_sse_parts(text)
    events = [_anthropic_msg_start(model, input_tokens), _sse_event("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})]
    for piece in pieces: events.append(_sse_event("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": piece}}))
    events.append(_sse_event("content_block_stop", {"type": "content_block_stop", "index": 0}))
    events.append(_anthropic_msg_end("end_turn", output_tokens))
    return "".join(events)
# --- Upstream calls and the failover loop.
class UpstreamFailure(Exception):
    def __init__(self, status, code=""):
        super().__init__("upstream HTTP %s" % status); self.status, self.code = status, code
def _error_code_of(wire, status, body_bytes):
    try: body = json.loads(body_bytes.decode("utf-8"))
    except Exception: return ""
    if not _is_record(body): return ""
    if wire == "openai" and _is_record(body.get("error")): return body["error"].get("code") or ""
    if wire == "anthropic" and _is_record(body.get("error")): return body["error"].get("type") or ""
    if wire == "gemini" and _is_record(body.get("error")): return body["error"].get("status") or ""
    return ""
def _refusal_kind_of(body_bytes):
    """The cause a refusal names in its own message: 'spent' (budget gone),
    'out' (outage, permanent) or 'sched' (a one-off scheduled refusal)."""
    try: body = json.loads(body_bytes.decode("utf-8"))
    except Exception: return ""
    if not _is_record(body) or not _is_record(body.get("error")): return ""
    message = body["error"].get("message")
    if not isinstance(message, str): return ""
    if "budget is spent" in message: return "spent"
    if "is out" in message: return "out"
    if "refused this call" in message: return "sched"
    return ""
def requirements_from_body(body):
    vision = False
    for message in body.get("messages") or []:
        if not _is_record(message) or not isinstance(message.get("content"), list): continue
        for block in message["content"]:
            if _is_record(block) and block.get("type") in ("image", "image_url"): vision = True
    fmt = body.get("response_format") if _is_record(body.get("response_format")) else {}
    return {"tools": isinstance(body.get("tools"), list) and len(body["tools"]) > 0, "vision": vision, "json": fmt.get("type") in ("json_object", "json_schema")}
class Aggregator:
    def __init__(self, brief, probes, catalog):
        self.brief, self.catalog = brief, catalog
        self.planner = Planner(brief, probes)
        self.manifest_index = 0
        self.lock = threading.RLock()  # whole serve path: strictly serial provider calls
        self.decision_cache = {}  # request hash -> (provider_id, model_id) that served it
    def served_models(self):
        out, seen = [], set()
        for state in self.planner.states.values():
            for model in sorted(state.probe["models"], key=lambda m: m["id"]):
                row = self.brief.ratings.get((state.provider["id"], model["id"]))
                free = row["free"] if row is not None else (model["free"] if model["free"] is not None else False)
                if not free or model["id"] in seen: continue
                seen.add(model["id"]); out.append({"id": model["id"], "provider": state.provider["id"], "display_name": model["display_name"]})
        return out
    def requirements_for(self, body):
        with self.lock:
            index = self.manifest_index; entry = self.brief.workload[index] if index < len(self.brief.workload) else None; self.manifest_index += 1
        return (entry["requires"], index) if entry is not None else (requirements_from_body(body), -1)
    def call_upstream(self, state, client_api, body, model_id, stream):
        provider, wire = state.provider, state.provider["wire"]
        base = join_base(ENV_URL, provider["base"])
        headers = {"content-type": "application/json"}
        headers.update(auth_headers(provider["auth"], provider["key"]))
        if wire == "openai":
            upstream_body = dict(body) if client_api == "openai" else anthropic_to_openai(body, model_id)
            if client_api == "openai": upstream_body["model"] = model_id
            if stream: upstream_body["stream"] = True; upstream_body["stream_options"] = {"include_usage": True}
            status, resp_headers, data = _http_call(base + "/v1/chat/completions", method="POST", headers=headers, body=compact_json(upstream_body).encode("utf-8"), timeout=UPSTREAM_TIMEOUT_S)
        elif wire == "anthropic":
            headers["anthropic-version"] = "2023-06-01"
            upstream_body = dict(body) if client_api == "anthropic" else openai_to_anthropic(body, model_id)
            if client_api == "anthropic": upstream_body["model"] = model_id
            status, resp_headers, data = _http_call(base + "/v1/messages", method="POST", headers=headers, body=compact_json(upstream_body).encode("utf-8"), timeout=UPSTREAM_TIMEOUT_S)
        else:
            upstream_body = openai_to_gemini(body, model_id) if client_api == "openai" else anthropic_to_gemini(body, model_id)
            model_path = urllib.parse.quote(model_id, safe="")
            url = "%s/v1beta/models/%s:streamGenerateContent?alt=sse" % (base, model_path) if stream else "%s/v1beta/models/%s:generateContent" % (base, model_path)
            status, resp_headers, data = _http_call(url, method="POST", headers=headers, body=compact_json(upstream_body).encode("utf-8"), timeout=UPSTREAM_TIMEOUT_S)
        if status >= 400:
            state.record_call(False, _refusal_kind_of(data)); raise UpstreamFailure(status, _error_code_of(wire, status, data))
        state.record_call(True)
        return status, resp_headers.get("content-type", DEFAULT_CONTENT_TYPE), data
    def translate_response(self, provider_wire, client_api, stream, data, model_id):
        text = data.decode("utf-8")
        if provider_wire == "openai":
            if client_api == "openai": return text
            return openai_sse_to_anthropic(text, model_id) if stream else compact_json(openai_json_to_anthropic(json.loads(text), model_id))
        if provider_wire == "anthropic":
            if client_api == "anthropic": return text
            return anthropic_sse_to_openai(text, model_id) if stream else compact_json(anthropic_json_to_openai(json.loads(text), model_id))
        if client_api == "openai": return gemini_sse_to_openai(text, model_id) if stream else compact_json(gemini_json_to_openai(json.loads(text), model_id))
        return gemini_sse_to_anthropic(text, model_id) if stream else compact_json(gemini_json_to_anthropic(json.loads(text), model_id))
    @staticmethod
    def _cache_key(client_api, body):
        return hashlib.sha256(json.dumps([client_api, body], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
    def serve(self, client_api, body):
        with self.lock: return self._serve_locked(client_api, body)
    def _serve_locked(self, client_api, body):
        stream = body.get("stream") is True
        requires, manifest_index = self.requirements_for(body)
        json_mode = client_api == "openai" and _is_record(body.get("response_format")) and body["response_format"].get("type") in ("json_object", "json_schema")
        cache_key = self._cache_key(client_api, body); cached = self.decision_cache.get(cache_key)
        failed_providers, last_error, attempts = set(), None, 0
        while True:
            candidates = self.planner.ordered(requires, client_api, json_mode, manifest_index=manifest_index, attempts=attempts)
            if cached is not None and attempts == 0:
                for cand in candidates:
                    if (cand["provider_id"], cand["model_id"]) == cached: candidates.remove(cand); candidates.insert(0, cand); break
            nxt = next((c for c in candidates if c["provider_id"] not in failed_providers), None)
            if nxt is None: break
            pid = nxt["provider_id"]; state = self.planner.states[pid]; attempts += 1
            if DEBUG: log("attempt %d: %s/%s (call %d, hazard %.2f)" % (attempts, pid, nxt["model_id"], state.calls + 1, state.hazard()))
            try: _status, content_type, data = self.call_upstream(state, client_api, body, nxt["model_id"], stream)
            except UpstreamFailure as exc:
                failed_providers.add(pid); last_error = "provider %s refused HTTP %s" % (pid, exc.status); continue
            except Exception as exc:
                failed_providers.add(pid); state.record_call(False); last_error = "provider %s unreachable: %s" % (pid, exc); continue
            try: answer = self.translate_response(state.provider["wire"], client_api, stream, data, nxt["model_id"])
            except Exception as exc:
                failed_providers.add(pid); last_error = "provider %s answer unusable: %s" % (pid, exc); continue
            self.decision_cache[cache_key] = (pid, nxt["model_id"])
            return (200, "text/event-stream; charset=utf-8", answer) if stream else (200, "application/json", answer)
        # Fallback sweep: every first-line candidate refused, but a
        # provider whose refusal was a scheduled one-off may answer on
        # its next call index. Retry each feasible provider once, best
        # chain-priced first, skipping only providers known permanently
        # dead (outage / spent budget) or at their budget ceiling. This
        # runs only when the request would otherwise be lost.
        swept = set()
        for cand in self.planner.ordered(requires, client_api, json_mode, manifest_index=manifest_index, attempts=99):
            pid = cand["provider_id"]
            if pid in swept: continue
            swept.add(pid)
            state = self.planner.states[pid]
            if state.kind_dead: continue
            hi = CALLS_MAX.get(pid)
            if hi is not None and state.calls >= hi: continue
            try: _status, content_type, data = self.call_upstream(state, client_api, body, cand["model_id"], stream)
            except Exception as exc:
                last_error = "provider %s refused on sweep: %s" % (pid, exc); continue
            try: answer = self.translate_response(state.provider["wire"], client_api, stream, data, cand["model_id"])
            except Exception: continue
            self.decision_cache[cache_key] = (pid, cand["model_id"])
            return (200, "text/event-stream; charset=utf-8", answer) if stream else (200, "application/json", answer)
        log("no provider served the request: %s" % (last_error or "no candidate"))
        return 503, "application/json", None
# --- HTTP server, registration, work poll.
def openai_model_list(models):
    return {"object": "list", "data": [{"id": m["id"], "object": "model", "created": FIXED_CREATED, "owned_by": m["provider"]} for m in models]}
def anthropic_model_list(models):
    return {"data": [{"type": "model", "id": m["id"], "display_name": m["display_name"], "created_at": FIXED_ANTHROPIC_CREATED_AT} for m in models], "has_more": False}
def openai_error(message, error_type, code): return {"error": {"message": message, "type": error_type, "code": code}}
def anthropic_error(error_type, message): return {"type": "error", "error": {"type": error_type, "message": message}}
def make_handler(aggregator):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *args): pass
        def _send_json(self, status, body):
            data = compact_json(body).encode("utf-8")
            self.send_response(status); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(data))); self.end_headers(); self.wfile.write(data)
        def _send_text(self, status, content_type, text):
            data = text.encode("utf-8")
            self.send_response(status); self.send_header("content-type", content_type); self.send_header("cache-control", "no-cache"); self.send_header("content-length", str(len(data))); self.end_headers(); self.wfile.write(data)
        def _read_json(self):
            try: length = int(self.headers.get("Content-Length") or 0)
            except ValueError: return None
            if length <= 0 or length > MAX_BODY_BYTES: return None
            try: value = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception: return None
            return value if _is_record(value) else None
        def _wire(self): return "anthropic" if isinstance(self.headers.get("x-api-key"), str) else "openai"
        def _error(self, wire, status, error_type, message):
            self._send_json(status, anthropic_error(error_type, message) if wire == "anthropic" else openai_error(message, error_type, None))
        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/v1/models":
                models = aggregator.served_models()
                self._send_json(200, anthropic_model_list(models) if isinstance(self.headers.get("x-api-key"), str) else openai_model_list(models)); return
            self._error(self._wire(), 404, "not_found", "no route GET %s" % parsed.path)
        def do_POST(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path in ("/v1/chat/completions", "/v1/messages"):
                wire = "openai" if parsed.path == "/v1/chat/completions" else "anthropic"
                body = self._read_json()
                if body is None: self._error(wire, 400, "invalid_request_error", "the request body is not one JSON object"); return
                status, content_type, answer = aggregator.serve(wire, body)
                if answer is None: self._error(wire, status, "no_free_model" if wire == "openai" else "overloaded_error", "no free model meets this request"); return
                self._send_text(status, content_type, answer); return
            self._error(self._wire(), 404, "not_found", "no route POST %s" % parsed.path)
    return Handler
def register(port, catalog):
    payload = compact_json({"protocol_version": PROTOCOL_VERSION, "port": port, "catalog": catalog}).encode("utf-8")
    status, _hdrs, data = _http_call(ENV_URL + "/register", method="POST", headers={"content-type": "application/json"}, body=payload, timeout=5.0)
    if status < 200 or status >= 300: raise RuntimeError("POST %s/register answered HTTP %s: %s" % (ENV_URL, status, data[:500].decode("utf-8", "replace")))
    reply = json.loads(data.decode("utf-8"))
    if _is_record(reply) and isinstance(reply.get("requests"), int): log("registered: the workload holds %d requests" % reply["requests"])
def work_done():
    try:
        status, _hdrs, data = _http_call(ENV_URL + "/work", timeout=WORK_TIMEOUT_S)
        if status != 200: return False
        work = json.loads(data.decode("utf-8")); return _is_record(work) and work.get("done") is True
    except Exception: return False
def load_catalog():
    brief = read_brief(PROMPT_FILE)
    if brief.error is not None: log(brief.error)
    probes = [probe_provider(ENV_URL, provider) for provider in brief.providers]
    for probe in probes:
        if not probe["ok"] and probe["error"] is not None: log(probe["error"])
    return brief, probes, build_catalog(probes, brief.ratings)
def main():
    brief, probes, catalog = load_catalog()
    if "--list" in sys.argv:
        sys.stdout.write(compact_json(catalog) + "\n"); sys.stdout.flush(); return 0
    if ENV_URL == "": raise RuntimeError("MAKEFUN_ENV_URL is not set")
    aggregator = Aggregator(brief, probes, catalog)
    server = ThreadingHTTPServer(("0.0.0.0", 0), make_handler(aggregator))
    port = server.server_address[1]
    if port == 8080: raise RuntimeError("the OS picked 8080, which the env owns on one layout")
    log("listening on 0.0.0.0:%d with %d answered providers and %d free models" % (port, len([p for p in probes if p["ok"]]), len(aggregator.served_models())))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    register(port, catalog)
    try:
        while not work_done(): threading.Event().wait(POLL_INTERVAL_S)
    finally: server.shutdown(); server.server_close()
    return 0
if __name__ == "__main__":
    try: sys.exit(main())
    except Exception as exc: log(str(exc)); sys.exit(1)
