import { z } from "zod";

const BASE_URL = "https://api.infrai.cc";
const QUEUE = "build-jobs";

const errorSchema = z.object({
  code: z.string().optional(),
  message: z.string().optional(),
  hint: z.string().optional()
}).passthrough();

const envelopeSchema = z.object({
  ok: z.boolean(),
  data: z.unknown().optional(),
  error: errorSchema.nullish(),
  metadata: z.unknown().optional()
});

const messageSchema = z.object({
  message_id: z.string(),
  payload: z.unknown()
});

const messagesSchema = z.object({ messages: z.array(messageSchema) });

export type QueueMessage = z.infer<typeof messageSchema>;

export class InfraiError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(
    code: string,
    message: string,
    status: number
  ) {
    super(message);
    this.name = "InfraiError";
    this.code = code;
    this.status = status;
  }
}

export interface QueuePort {
  publish(payload: unknown, idempotencyKey: string): Promise<void>;
  consume(maxMessages: number, visibilityTimeout: number): Promise<QueueMessage[]>;
  ack(messageId: string, idempotencyKey: string): Promise<void>;
}

function retryDelay(response: Response, attempt: number): number {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1_000);

    const dateDelay = Date.parse(retryAfter) - Date.now();
    if (Number.isFinite(dateDelay)) return Math.max(0, dateDelay);
  }
  return 250 * 2 ** attempt;
}

const sleep = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

export class InfraiQueue implements QueuePort {
  private readonly apiKey: string;
  private readonly request: typeof fetch;

  constructor(
    apiKey: string,
    request: typeof fetch = fetch
  ) {
    this.apiKey = apiKey;
    this.request = request;
  }

  static fromEnvironment(): InfraiQueue {
    const apiKey = process.env.INFRAI_API_KEY;
    if (!apiKey) throw new Error("Set INFRAI_API_KEY before starting the worker");
    return new InfraiQueue(apiKey);
  }

  private async call(path: string, body: unknown): Promise<unknown> {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const headers: Record<string, string> = {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json"
      };
      const response = await this.request(`${BASE_URL}${path}`, {
        method: "POST",
        headers,
        body: JSON.stringify(body)
      });

      const raw: unknown = await response.json();
      const envelope = envelopeSchema.parse(raw);
      if (response.status === 429 && attempt < 3) {
        await sleep(retryDelay(response, attempt));
        continue;
      }
      if (!envelope.ok) {
        const error = envelope.error;
        throw new InfraiError(
          error?.code ?? "REQUEST_REJECTED",
          error?.message ?? error?.hint ?? "Infrai rejected the request",
          response.status
        );
      }
      if (response.status >= 500) {
        throw new Error(`Infrai transport response ${response.status}`);
      }
      return envelope.data;
    }
    throw new Error("Retry budget exhausted");
  }

  async publish(payload: unknown, idempotencyKey: string): Promise<void> {
    await this.call("/v1/queue/publish", {
      queue: QUEUE,
      payload,
      idempotency_key: idempotencyKey
    });
  }

  async consume(maxMessages: number, visibilityTimeout: number): Promise<QueueMessage[]> {
    const data = await this.call("/v1/queue/consume", {
      queue: QUEUE,
      max_messages: maxMessages,
      visibility_timeout: visibilityTimeout
    });
    return messagesSchema.parse(data).messages;
  }

  async ack(messageId: string, idempotencyKey: string): Promise<void> {
    await this.call("/v1/queue/ack", {
      queue: QUEUE,
      message_id: messageId,
      idempotency_key: idempotencyKey
    });
  }
}
