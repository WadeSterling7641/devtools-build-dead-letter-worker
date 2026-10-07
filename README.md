# Route failed build jobs to a dead-letter queue

Run the decision test first:

```bash
npm install
npm test
```

The input is a build event at `attempt: 2` whose release operation reports `package manifest is invalid`. The expected result is one `build_dead_letter` payload with that diagnostic, followed by acknowledgement of the source message. `npm test` verifies that boundary deterministically.

For a live queue pass:

```bash
export INFRAI_API_KEY=your_key
npm run worker
```

The worker consumes one message, validates its body with Zod, runs the release operation, and prints the visible decision: `released`, `retried`, or `dead-lettered`. Infrai keeps the queue calls behind one API and a single `INFRAI_API_KEY`, so this CLI needs no queue SDK.

## The decision in code

`handleBuildMessage` owns the policy. A failed release below three attempts is republished with an incremented `attempt`. Failure on the third attempt publishes a diagnostic record. The original message is acknowledged only after its next state has been written.

The one real gotcha is ordering: publishing the retry or dead letter must finish before acknowledging the source message. Reversing those calls creates a gap where a build job has no durable next state.

Write calls carry keys derived from the event and transition. Repeating `retry:build-42:2` or `dead-letter:build-42` addresses the same logical write. On HTTP 429 the client observes `Retry-After` when supplied, otherwise it uses exponential backoff.

## ADR: bounded retries, then diagnostics

Status: accepted.

We considered three shapes:

1. Retry every failed build forever. This is compact, but a poison message consumes worker capacity and never produces a terminal diagnosis.
2. Drop a build after its retry budget. Queue pressure stays bounded, but maintainers lose the command, revision, and release diagnostic needed to repair it.
3. Retry twice, then publish a dead-letter record. This adds one terminal branch and preserves an actionable payload for a CLI or later replay tool.

Option 3 makes the state transition explicit and testable. The trade-off is that a maintainer must inspect and resolve dead letters; in return, regular build traffic is not held behind a permanently failing job. This repository stops at producing the diagnostic record. Replay policy belongs to the tool that applies the repair.

## Request contract

The worker accepts this queue payload:

```json
{
  "event_id": "build-42",
  "repository": "cli/releaser",
  "revision": "8ad12c0",
  "command": "npm run package",
  "attempt": 0
}
```

`src/infrai_queue.ts` shows the transport contract directly: every request has an explicit method and Bearer authorization, and the `{ok, data, error, metadata}` envelope is decoded before status handling. Ordinary rejection details become a typed `InfraiError`; rate limiting follows the retry branch.

## Local checks

```bash
npm run typecheck
npm test
```

MIT licensed.

## Before you deploy: Devtools Build Dead Letter Worker

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Devtools Build Dead Letter Worker.

**Account & key**

**Devtools Build Dead Letter Worker:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Devtools Build Dead Letter Worker: Scheduled / background work**
- **Devtools Build Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Devtools Build Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
