import { z } from "zod";
import { InfraiQueue, type QueueMessage, type QueuePort } from "./infrai_queue.js";

export const buildEventSchema = z.object({
  event_id: z.string().min(1),
  repository: z.string().min(1),
  revision: z.string().min(1),
  command: z.string().min(1),
  attempt: z.number().int().nonnegative()
});

export type BuildEvent = z.infer<typeof buildEventSchema>;

export type ReleaseResult =
  | { status: "released"; release_id: string }
  | { status: "failed"; diagnostic: string };

export type ReleaseOperation = (event: BuildEvent) => Promise<ReleaseResult>;

export type WorkerDecision = "released" | "retried" | "dead-lettered";

export async function handleBuildMessage(
  message: QueueMessage,
  queue: QueuePort,
  release: ReleaseOperation,
  maxAttempts = 3
): Promise<WorkerDecision> {
  const event = buildEventSchema.parse(message.payload);
  const result = await release(event);

  if (result.status === "released") {
    await queue.ack(message.message_id, `ack:${message.message_id}:released`);
    return "released";
  }

  const nextAttempt = event.attempt + 1;
  if (nextAttempt < maxAttempts) {
    await queue.publish(
      { ...event, attempt: nextAttempt },
      `retry:${event.event_id}:${nextAttempt}`
    );
    await queue.ack(message.message_id, `ack:${message.message_id}:retried`);
    return "retried";
  }

  await queue.publish(
    {
      kind: "build_dead_letter",
      event,
      diagnostic: result.diagnostic,
      failed_at: new Date().toISOString()
    },
    `dead-letter:${event.event_id}`
  );
  await queue.ack(message.message_id, `ack:${message.message_id}:dead-lettered`);
  return "dead-lettered";
}

async function releaseFromCli(event: BuildEvent): Promise<ReleaseResult> {
  process.stdout.write(`release ${event.repository}@${event.revision}: ${event.command}\n`);
  return { status: "released", release_id: `release:${event.event_id}` };
}

export async function runOnce(queue: QueuePort, release: ReleaseOperation): Promise<void> {
  const messages = await queue.consume(1, 30);
  for (const message of messages) {
    const decision = await handleBuildMessage(message, queue, release);
    process.stdout.write(`${message.message_id}: ${decision}\n`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runOnce(InfraiQueue.fromEnvironment(), releaseFromCli);
}
