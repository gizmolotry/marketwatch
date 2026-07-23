# Historical wallet case replay

## Purpose

A historical wallet replay asks a concrete signal question: **how strongly does a documented case wallet stand out against the complete same-market population at a fixed cutoff?** The output leads with signal classification, confidence, coverage, and review priority. Final disposition belongs to a human reviewer.

The two replay modes remain distinct:

| Mode | Fact admission rule at cutoff `T` | Output |
|---|---|---|
| Operational | `event_time <= T` and `available_at <= T` | What the running system could use at `T` |
| Hindsight reconstruction | `event_time <= T`; availability may be later | A clearly marked retrospective activity signal |

Both modes preserve immutable raw lineage, retrieval receipts, exact queries, source watermarks, coverage, and both clocks. A late retrieval never becomes contemporaneous evidence.

## Van Dyke / Burdensome-Mix result

Public Polymarket proxy wallet:

```text
0x31a56e9e690c621ed21de08cb559e9524cdb8ed9
```

The [CFTC civil complaint](https://www.cftc.gov/media/13761/EnfGannonKenVanDykeComplaint042326/download) identifies the handle "Burdensome-Mix," a masked `0x31a5...8ed9` wallet, the relevant Polymarket purchases, and the alleged chronology. The [DOJ release](https://www.justice.gov/opa/pr/us-soldier-charged-using-classified-information-profit-prediction-market-bets) provides separately timestamped legal context. Legal records and resolution outcomes do not enter the activity features.

The same-market replay uses the external announcement-anchored cutoff `2026-01-03T09:20:59Z`. Its frozen capture contains 21,785 unique fills from 3,449 wallets. The result is a **high-priority signal**:

- composite activity rank: **33 of 3,449**;
- Yes-buy-notional rank: **7 of 1,339**;
- gross-market-notional rank: **32 of 3,449**.

The composite uses the pre-existing feature policy, exact leave-one-out peer calibration, and tie-safe empirical tails. The fixed generic bands are top 1% = `high`, top 5% = `elevated`, and the remainder = `routine`; an unavailable population percentile is `insufficient_data`. These thresholds are included in the policy hash and were not selected for this case.

The trade source does not expose the exact market publication timestamp. The earliest captured same-market trade at `2025-12-12T01:20:24Z` is retained as a documented public-existence-no-later-than bound. That bound is sufficient for this hindsight same-market comparison, while `focus_published_at` remains explicitly unmapped. Operational scoring does not use this fallback.

## Ethereum adapter benchmark

The Wahi/Ramani address `0x1C84a6d53F8950cd06a4016E5f547a089Dd7B6Fb` remains a future generic Ethereum replay benchmark. The [SEC complaint](https://www.sec.gov/files/litigation/complaints/2022/comp-pr2022-127.pdf), [Japan FSA research report](https://www.fsa.go.jp/common/about/research/20230427/20230427_report_digitalassets.pdf), and [SEC final-judgment release](https://www.sec.gov/enforcement-litigation/litigation-releases/lr-25947) provide independently retained mapping and legal context. The current Polymarket collector does not implement this Ethereum replay.

## Required output

Every case replay should lead with:

- classification (`high`, `elevated`, `routine`, or `insufficient_data`), confidence, coverage, and review priority;
- composite and interpretable component ranks;
- cutoff and distinct event/availability clocks;
- raw hashes, receipts, exact query scope, source watermarks, and coverage;
- case mapping and legal context kept outside the activity features;
- one concise `decision_owner: human_reviewer` boundary.
