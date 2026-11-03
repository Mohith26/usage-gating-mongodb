// Usage gating on MongoDB: three ways to charge a video against a billing period.
// Runs against an in-memory single-node replica set (mongodb-memory-server).
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { MongoClient } from "mongodb";

const TRIALS = Number(process.env.TRIALS ?? 20);
const ALLOWANCE = 10; // minutes in the billing period
const VIDEOS = 40; // distinct 1-minute videos requested at once
const RETRIES = 1; // each request is sent again, like a client retry after a timeout
const APP_WORK_MS = Number(process.env.APP_WORK_MS ?? 0); // optional delay between check and write
const JITTER_MS = Number(process.env.JITTER_MS ?? 0); // spread request arrivals over 0..JITTER_MS ms

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A: read the balance, then write the event and decrement it.
async function chargeNaive(db, periodId, videoId, minutes) {
  const period = await db.collection("billing_periods").findOne({ _id: periodId });
  if (period.remaining < minutes) return "rejected";
  if (APP_WORK_MS) await sleep(APP_WORK_MS);
  await db.collection("usage_events").insertOne({ periodId, videoId, minutes, at: new Date() });
  await db.collection("billing_periods").updateOne({ _id: periodId }, { $inc: { remaining: -minutes } });
  return "charged";
}

// B: conditional decrement in one atomic update, then log the event. No idempotency key.
async function chargeAtomic(db, periodId, videoId, minutes) {
  if (APP_WORK_MS) await sleep(APP_WORK_MS);
  const res = await db.collection("billing_periods").updateOne(
    { _id: periodId, remaining: { $gte: minutes } },
    { $inc: { remaining: -minutes } },
  );
  if (res.modifiedCount === 0) return "rejected";
  await db.collection("usage_events").insertOne({ periodId, videoId, minutes, at: new Date() });
  return "charged";
}

// C: transaction with an idempotency key. The event insert hits a unique index on
// (periodId, videoId), so a retry for the same video is a no-op, and the balance
// decrement is conditional, so the period can't go negative. Both commit or neither.
async function chargeTxn(client, db, periodId, videoId, minutes) {
  const session = client.startSession();
  try {
    let outcome;
    await session.withTransaction(async () => {
      if (APP_WORK_MS) await sleep(APP_WORK_MS);
      try {
        await db.collection("usage_events_keyed").insertOne(
          { periodId, videoId, minutes, at: new Date() }, { session });
      } catch (e) {
        // A write error aborts the transaction on the server; end it on our side too.
        if (e.code === 11000) { outcome = "duplicate"; await session.abortTransaction(); return; }
        throw e;
      }
      const res = await db.collection("billing_periods").updateOne(
        { _id: periodId, remaining: { $gte: minutes } },
        { $inc: { remaining: -minutes } }, { session });
      if (res.modifiedCount === 0) {
        outcome = "rejected";
        await session.abortTransaction();
        return;
      }
      outcome = "charged";
    });
    return outcome;
  } finally {
    await session.endSession();
  }
}

async function trial(client, db, strategy, t) {
  const periodId = `${strategy}-${t}`;
  await db.collection("billing_periods").insertOne({ _id: periodId, allowance: ALLOWANCE, remaining: ALLOWANCE });
  const events = strategy === "C_txn_idempotent" ? "usage_events_keyed" : "usage_events";
  const calls = [];
  for (let v = 0; v < VIDEOS; v++) {
    for (let r = 0; r <= RETRIES; r++) {
      const videoId = `video-${v}`;
      const start = JITTER_MS ? sleep(Math.random() * JITTER_MS) : Promise.resolve();
      calls.push(start.then(() => {
        if (strategy === "A_read_then_write") return chargeNaive(db, periodId, videoId, 1);
        if (strategy === "B_atomic_no_key") return chargeAtomic(db, periodId, videoId, 1);
        return chargeTxn(client, db, periodId, videoId, 1);
      }));
    }
  }
  await Promise.all(calls);
  const period = await db.collection("billing_periods").findOne({ _id: periodId });
  const log = await db.collection(events).find({ periodId }).toArray();
  const perVideo = new Map();
  for (const e of log) perVideo.set(e.videoId, (perVideo.get(e.videoId) ?? 0) + e.minutes);
  const charged = log.reduce((s, e) => s + e.minutes, 0);
  return {
    charged,                                   // minutes in the usage log
    overspend: Math.max(0, charged - ALLOWANCE),
    finalRemaining: period.remaining,
    doubleCharged: [...perVideo.values()].filter((m) => m > 1).length,
    ledgerMismatch: ALLOWANCE - charged !== period.remaining,
  };
}

async function balanceReadBenchmark(db) {
  const rows = [];
  for (const n of [1_000, 10_000, 100_000]) {
    const periodId = `bench-${n}`;
    await db.collection("billing_periods").insertOne({ _id: periodId, remaining: 0 });
    const docs = Array.from({ length: n }, (_, i) => ({ periodId, videoId: `v${i}`, minutes: 1, at: new Date() }));
    for (let i = 0; i < docs.length; i += 10_000) await db.collection("bench_events").insertMany(docs.slice(i, i + 10_000));
    const time = async (fn) => {
      const xs = [];
      for (let i = 0; i < 30; i++) { const t0 = performance.now(); await fn(); xs.push(performance.now() - t0); }
      xs.sort((a, b) => a - b);
      return xs[15];
    };
    const viaDoc = await time(() => db.collection("billing_periods").findOne({ _id: periodId }));
    const viaAgg = await time(() => db.collection("bench_events").aggregate([
      { $match: { periodId } }, { $group: { _id: null, used: { $sum: "$minutes" } } }]).toArray());
    rows.push({ events: n, balanceDocMs: viaDoc, aggregateEventsMs: viaAgg });
  }
  return rows;
}

const rs = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: "wiredTiger" } });
const client = await MongoClient.connect(rs.getUri(), { maxPoolSize: 50 });
const db = client.db("billing");
await db.collection("usage_events_keyed").createIndex({ periodId: 1, videoId: 1 }, { unique: true });
await db.collection("usage_events").createIndex({ periodId: 1 });
await db.collection("bench_events").createIndex({ periodId: 1 });

console.log(`allowance ${ALLOWANCE} min, ${VIDEOS} one-minute videos x ${RETRIES + 1} sends, ${TRIALS} trials, arrivals spread over ${JITTER_MS} ms, app work ${APP_WORK_MS} ms`);
const summary = {};
for (const strategy of ["A_read_then_write", "B_atomic_no_key", "C_txn_idempotent"]) {
  const rs_ = [];
  for (let t = 0; t < TRIALS; t++) rs_.push(await trial(client, db, strategy, t));
  const avg = (k) => rs_.reduce((s, r) => s + r[k], 0) / rs_.length;
  const max = (k) => Math.max(...rs_.map((r) => r[k]));
  summary[strategy] = {
    trialsWithOverspend: rs_.filter((r) => r.overspend > 0).length,
    meanChargedMin: avg("charged"), maxChargedMin: max("charged"),
    meanDoubleChargedVideos: avg("doubleCharged"), maxDoubleChargedVideos: max("doubleCharged"),
    minFinalRemaining: Math.min(...rs_.map((r) => r.finalRemaining)),
    trialsWhereLogAndBalanceDisagree: rs_.filter((r) => r.ledgerMismatch).length,
  };
}
console.table(summary);
if (process.env.SKIP_BENCH !== "1") console.table(await balanceReadBenchmark(db));
await client.close();
await rs.stop();
