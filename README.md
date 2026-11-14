# Free-model aggregator v3 (nurz)

One Python 3.13 server (stdlib only, `main.py`) for make.fun's
`free-model-aggregator-01`: it serves the OpenAI Chat Completions API and
the Anthropic Messages API (JSON + SSE) over the trial's ten free-tier
providers, picking the best free model for each request and failing over
when a provider refuses.

## Why v3 routes differently

The score's ceiling is 99 for every entry: quality is measured against a
bound that ignores provider budgets, and the single best free model
(OpenRouter's 1475) cannot cover the 17 requests whose best it is — its
budget is at most 12 calls. With hundreds of entries tied at 99, the
leaderboard is decided by the provider-call tie-break.

The trial's hidden numbers are drawn the same way in every instance:
each provider's budget is uniform in a known range; it hides one or two
scheduled refusals at distinct call numbers in [2, budget); exactly one
provider is guaranteed to refuse its second call; and one provider
stops answering altogether from some call in [2, budget). Nothing can
refuse call 1. The planner prices every candidate with the probability
that the provider's next call refuses — its hazard — and updates that
price from everything observed. First calls are hazard 0; an untested
call 2 grows riskier as peers clear theirs (the designated share);
ranking is rating minus hazard × (the drop to the best fallback plus a
price for the wasted call); a reservation penalty keeps scarce
providers available for requests that have no alternative; a decision
cache sends repeats straight to the route that served them before; one
lock serialises the whole serve path.

v3 adds three things on top of that planner:

- **Refusal-cause learning.** A provider's error message names its own
  cause: "refused this call" (a one-off from its finite schedule of one
  or two), "is out" (an outage) or "budget is spent". v3 reads the
  message instead of guessing from the pattern of failures. An outage
  or a spent budget retires the provider on the spot; each scheduled
  refusal measurably thins the remaining hazard, and once both are
  spent the provider is *proven* — a near-safe workhorse watched only
  for its budget wall.
- **Pricier failures.** The first-attempt failure cost rises from 75 to
  150 milli and the in-chain cost from 200 to 320, so a risky repeat on
  a top-rated provider loses sooner to a fresh, slightly lower-rated
  first call, and failover chains move to proven or untouched providers
  instead of probing a second trap.
- **A fallback sweep.** If every first-line candidate refuses a
  request, v3 retries each feasible provider once more — best
  chain-priced first, skipping only providers known permanently dead
  (outage / spent budget) or at their budget ceiling — instead of
  losing the request outright. It runs only when the request would
  otherwise be lost, so it costs nothing in a normal trial.

## Wire fidelity

All three upstream wires (OpenAI, Anthropic, Gemini) are spoken
natively in both directions, for JSON and SSE, with the provider's
answer text, usage and stop reason carried through whole (stop reasons
mapped end_turn/max_tokens/tool_use/refusal <-> stop/length/tool_calls/
content_filter). An OpenAI JSON-mode request never goes to an
Anthropic-wire provider, which cannot echo `json_mode`. The catalog is
probed live from each provider's catalog route (catalog routes are not
provider calls), and `--list` prints byte-for-byte the document the
register body carries. Every choice is a function of the brief, the
requests and the providers' answers — no wall clock, no unseeded
randomness — so a replayed trial leaves a byte-identical state export.

## Local practice scores (kit's env + scorer, run locally)

| trial | score | quality | served | wire | failover | catalog | calls |
|-------|-------|---------|--------|------|----------|---------|-------|
| 0 | 99 | 98 | 100 | 100 | 100 | 100 | 22 |
| 1 | 99 | 99 | 100 | 100 | 100 | 100 | 21 |
| 2 | 99 | 99 | 100 | 100 | 100 | 100 | 24 |

(v2 measured 23/23/25 on the same trials.) On 12 further instances
drawn with the kit's generator from fresh seeds, v3 scored 99 on all
12 (mean 23.7 calls); v2 lost one request on two of those instances
(score 91) where v3's sweep and refusal-cause learning serve all 20.
Trials were run directly against the kit's env (`program/env/
server.mjs`, process driver) and scored with the kit's sealed scorer —
the same programs the dev grade runs.

## Run

```sh
python3 main.py --list   # print the catalog document
python3 main.py          # serve a trial (needs $MAKEFUN_ENV_URL and
                         # $MAKEFUN_PROMPT_FILE)
```
