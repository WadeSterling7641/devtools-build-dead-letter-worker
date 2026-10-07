import assert from "node:assert/strict";
import test from "node:test";
import type { QueuePort } from "../src/infrai_queue.js";
import { handleBuildMessage } from "../src/queue_worker.js";

test("a poison build becomes a diagnostic dead letter on its third attempt", async () => {
  const published: Array<{ payload: unknown; key: string }> = [];
  const acknowledged: string[] = [];
  const queue: QueuePort = {
    publish: async (payload, key) => { published.push({ payload, key }); },
    consume: async () => [],
    ack: async (messageId) => { acknowledged.push(messageId); }
  };

  const decision = await handleBuildMessage(
    {
      message_id: "msg-17",
      payload: {
        event_id: "build-42",
        repository: "cli/releaser",
        revision: "8ad12c0",
        command: "npm run package",
        attempt: 2
      }
    },
    queue,
    async () => ({ status: "failed", diagnostic: "package manifest is invalid" })
  );

  assert.equal(decision, "dead-lettered");
  assert.equal(published.length, 1);
  assert.equal(published[0]?.key, "dead-letter:build-42");
  assert.deepEqual(acknowledged, ["msg-17"]);
  assert.deepEqual(published[0]?.payload, {
    kind: "build_dead_letter",
    event: {
      event_id: "build-42",
      repository: "cli/releaser",
      revision: "8ad12c0",
      command: "npm run package",
      attempt: 2
    },
    diagnostic: "package manifest is invalid",
    failed_at: (published[0]?.payload as { failed_at: string }).failed_at
  });
});
