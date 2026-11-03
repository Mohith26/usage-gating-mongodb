# usage-gating-mongodb

A small experiment on one part of the billing design in Synthesia's post [How We Built a Billing System That Scaled with Us from $40M ARR to ~$140M](https://www.synthesia.io/post/how-we-scaled-our-billing-system-from-40m-to-140m-arr): enforce usage limits with a current-balance document on the billing period plus an immutable usage log, on MongoDB, so a video can't be charged twice and a period can't go below zero.

I wanted to see how much the implementation details matter, so `loop.mjs` charges videos against a 10-minute billing period three ways:

- **A, read then write:** read `remaining`, and if there's room, insert a usage event and `$inc` the balance.
- **B, atomic decrement:** one `updateOne({remaining: {$gte: cost}}, {$inc: ...})`, then insert the event. No idempotency key.
- **C, transaction + idempotency key:** in a transaction, insert the event into a log with a unique index on `(periodId, videoId)`, then do the conditional decrement. A retry for the same video hits the unique index and is a no-op.

Each trial sends 40 one-minute videos at once, and every request is sent twice (like a client retry after a timeout), so 80 calls. 20 trials per setting, MongoDB 7.0.24 single-node replica set in memory.

## Results

| arrivals | strategy | trials over limit | minutes charged (mean / max, limit 10) | videos charged twice (mean / max) | lowest final balance |
|---|---|---:|---|---|---:|
| all at once | A read then write | 20/20 | 80 / 80 | 40 / 40 | -70 |
| | B atomic decrement | 0/20 | 10 / 10 | 2.05 / 4 | 0 |
| | C txn + key | 0/20 | 10 / 10 | 0 / 0 | 0 |
| spread over 50 ms | A | 20/20 | 27.4 / 61 | 5.5 / 23 | -51 |
| | B | 0/20 | 10 / 10 | 0.55 / 2 | 0 |
| | C | 0/20 | 10 / 10 | 0 / 0 | 0 |
| spread over 500 ms | A | 12/20 | 11.1 / 13 | 0.7 / 3 | -3 |
| | B | 0/20 | 10 / 10 | 0.55 / 2 | 0 |
| | C | 0/20 | 10 / 10 | 0 / 0 | 0 |

- Read-then-write overspends whenever requests overlap, even when they're spread over half a second.
- The atomic decrement fixes overspending but still charges retried videos twice, so the customer gets fewer videos for the same minutes. That's the case the post's "users are not double-charged for the same video" guarantee is about, and it needs a key on the video, not just an atomic balance.
- The transaction with a unique `(periodId, videoId)` index had no overspend and no double charges in any trial.

The post also says a stored balance beats summing the usage log on every check. Median time to read the balance for one period (30 reads each, indexed on `periodId`):

| usage events in the period | balance document | `$group` sum over events |
|---:|---:|---:|
| 1,000 | 0.3 ms | 1.8 ms |
| 10,000 | 0.4 ms | 11.8 ms |
| 100,000 | 0.3 ms | 111.7 ms |

The table is from the first block of [results.md](results.md) (one command per row group). The run with the read benchmark is a separate run, so its trial numbers differ slightly. results.md also has a run with 5 ms of app work between check and write.

## Run it

```
npm install
node loop.mjs                    # default: all requests at once, plus the read benchmark
JITTER_MS=50 SKIP_BENCH=1 node loop.mjs
JITTER_MS=50 APP_WORK_MS=5 SKIP_BENCH=1 node loop.mjs
```

`mongodb-memory-server` downloads a MongoDB binary the first time; no database setup or account needed.

## Limits

- One node, in memory, on a laptop. Timings are relative, not production numbers.
- I don't inject crashes, so the "log and balance agree" check is always true here. B would drift if the process died between its two writes; C wouldn't, because both writes commit together.
- I didn't reproduce the Bucket pattern for the usage log; the keyed log here is one document per event.
- This is my reading of the post, not Synthesia's code.
