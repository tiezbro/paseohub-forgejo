import assert from "node:assert/strict";
import { describe, it } from "vitest";
import { createMemoryDatabase } from "../../db/memory.js";
import type { DurableProviderEvent } from "../../db/types.js";
import { summarizeTrigger } from "../../projects/activity-summary.js";
import {
  deliveryByName,
  loadForgejoContractFixtures,
} from "../../providers/forgejo/fake-server.js";
import { createActiveProjectConfiguration } from "../../test-utils/project-configuration.js";
import { isAcceptedTriggerProviderMatch } from "../index.js";
import { dispatchForgejoClaimed } from "./dispatch.js";
import {
  createForgejoCommentConsumer,
  createForgejoCommentTriggerProvider,
  type ForgejoCommentDispatchTarget,
} from "./comments.js";
import type { ForgejoCommentReactionClient } from "./comment-reactions.js";
import type { ForgejoVerifiedDelivery } from "./webhook.js";

const CONNECTION = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  slug: "acme-forgejo",
  instanceId: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
};

const ISSUE_COMMENT_HTML_URL =
  "https://forgejo.example.test/t00org/t00repo/issues/3#issuecomment-2";

describe("Forgejo comment workflow consumer", () => {
  it("fans a comment out to zero, one, or many routes; duplicate consume enqueues again", async () => {
    const listed: number[] = [];
    const zeroEnqueued: string[] = [];
    const zeroConsumer = createForgejoCommentConsumer({
      enqueue: async (trigger) => {
        zeroEnqueued.push(`${trigger.projectId}:${trigger.source}`);
      },
      listTargets: async () => {
        listed.push(1);
        return [];
      },
    });
    const zero = await dispatchForgejoClaimed({
      delivery: await verified("issue-comment-created"),
      receiptId: "receipt-zero",
      connection: CONNECTION,
      consumers: { workflow: zeroConsumer },
    });
    assert.equal(zero.workflow.status, "succeeded");
    assert.equal(listed.length, 1);
    assert.equal(zeroEnqueued.length, 0);

    const oneEnqueued: string[] = [];
    const oneConsumer = createForgejoCommentConsumer({
      enqueue: async (trigger) => {
        oneEnqueued.push(`${trigger.projectId}:${trigger.source}`);
        assert.equal(trigger.source, "forgejo.issue_comment_created");
      },
      listTargets: async () => [target("project-a", "rev-1")],
    });
    await dispatchForgejoClaimed({
      delivery: await verified("issue-comment-created"),
      receiptId: "receipt-one",
      connection: CONNECTION,
      consumers: { workflow: oneConsumer },
    });
    assert.deepEqual(oneEnqueued, ["project-a:forgejo.issue_comment_created"]);

    const manyEnqueued: string[] = [];
    const manyConsumer = createForgejoCommentConsumer({
      enqueue: async (trigger) => {
        manyEnqueued.push(`${trigger.projectId}:${trigger.source}`);
      },
      listTargets: async () => [target("project-a", "rev-1"), target("project-b", "rev-2")],
    });
    await dispatchForgejoClaimed({
      delivery: await verified("issue-comment-created"),
      receiptId: "receipt-many",
      connection: CONNECTION,
      consumers: { workflow: manyConsumer },
    });
    assert.deepEqual(manyEnqueued, [
      "project-a:forgejo.issue_comment_created",
      "project-b:forgejo.issue_comment_created",
    ]);

    // Duplicate consume does not add extra uniqueness beyond enqueue.
    await dispatchForgejoClaimed({
      delivery: await verified("issue-comment-created"),
      receiptId: "receipt-dup",
      connection: CONNECTION,
      consumers: { workflow: manyConsumer },
    });
    assert.equal(manyEnqueued.length, 4);
  });

  it("ignores push and issues-opened complete events", async () => {
    let called = false;
    const consumer = createForgejoCommentConsumer({
      enqueue: async () => {
        called = true;
      },
      listTargets: async () => {
        called = true;
        return [];
      },
    });
    for (const name of ["push-default-branch", "issues-opened"] as const) {
      const observation = await dispatchForgejoClaimed({
        delivery: await verified(name),
        receiptId: `receipt-${name}`,
        connection: CONNECTION,
        consumers: { workflow: consumer },
      });
      assert.equal(observation.workflow.status, "succeeded");
    }
    assert.equal(called, false);
  });
});

describe("Forgejo comment trigger provider", () => {
  it("matches issue and pull-request comment-created families without crossing them", async () => {
    const issueMatch = await matchNamed("issue-comment-created", "forgejo.issue_comment_created");
    if (typeof issueMatch === "string") throw new Error(issueMatch);
    const issue = issueMatch[0];
    if (issue === undefined || !isAcceptedTriggerProviderMatch(issue)) {
      throw new Error("expected accepted issue comment match");
    }
    assert.deepEqual(issue.triggerContext.reactionSubject, { kind: "comment", id: 2 });
    const ready = await createProvider("forgejo.issue_comment_created");
    const materialized = await ready.provider.materializeContext?.({
      executionId: "exec-1",
      organizationId: ready.project.organizationId,
      projectId: ready.project.id,
      providerEventReceiptId: "receipt-1",
      triggerContext: issue.triggerContext,
    });
    assert.deepEqual(materialized, issue.triggerContext.event);

    const issueCross = await matchNamed(
      "issue-comment-created",
      "forgejo.pull_request_comment_created",
    );
    assert.equal(issueCross, "no_trigger_for_source");

    const pullMatch = await matchNamed(
      "pull-request-comment-created",
      "forgejo.pull_request_comment_created",
    );
    if (typeof pullMatch === "string") throw new Error(pullMatch);
    const pull = pullMatch[0];
    if (pull === undefined || !isAcceptedTriggerProviderMatch(pull)) {
      throw new Error("expected accepted pull request comment match");
    }
    assert.deepEqual(pull.triggerContext.reactionSubject, { kind: "comment", id: 5 });
    assert.notEqual(pull.triggerContext.reactionSubject?.id, 4);

    const pullCross = await matchNamed(
      "pull-request-comment-created",
      "forgejo.issue_comment_created",
    );
    assert.equal(pullCross, "no_trigger_for_source");
  });

  it("projects eyes, +1, and -1 onto the exact comment id", async () => {
    const posted: string[] = [];
    const { provider, match } = await matchWithReactions("issue-comment-created", posted);
    if (typeof match === "string") throw new Error(match);
    const accepted = match[0];
    if (accepted === undefined || !isAcceptedTriggerProviderMatch(accepted)) {
      throw new Error("expected accepted match");
    }
    assert.deepEqual(accepted.triggerContext.reactionSubject, { kind: "comment", id: 2 });
    const eyes = await provider.onDispatchAccepted?.(
      accepted.triggerContext,
      accepted.outputContext,
    );
    assert.deepEqual(eyes, { content: "eyes", kind: "comment", id: 2 });
    const plus = await provider.onAgentExecutionCompleted?.(
      accepted.triggerContext,
      accepted.outputContext,
      { status: "succeeded" },
    );
    assert.deepEqual(plus, { content: "+1", kind: "comment", id: 2 });
    const minus = await provider.onAgentExecutionFailed?.(
      accepted.triggerContext,
      accepted.outputContext,
      "failed",
    );
    assert.deepEqual(minus, { content: "-1", kind: "comment", id: 2 });
    const terminated = await provider.onMachineTerminated?.(accepted.triggerContext, "killed");
    assert.deepEqual(terminated, { content: "-1", kind: "comment", id: 2 });
    assert.deepEqual(posted, ["comment:2:eyes", "comment:2:+1", "comment:2:-1", "comment:2:-1"]);
  });

  it("summarizes a comment trigger with the native comment html_url", async () => {
    const fixtures = await loadForgejoContractFixtures();
    const delivery = deliveryByName(fixtures, "issue-comment-created");
    const summary = summarizeTrigger("forgejo.issue_comment", {
      headers: {
        "x-forgejo-delivery": delivery.headers["x-forgejo-delivery"],
        "x-forgejo-event": delivery.event,
        "x-forgejo-event-type": delivery.eventType,
      },
      raw: delivery.raw,
    });
    assert.equal(summary.provider, "forgejo");
    assert.equal(summary.externalUrl, ISSUE_COMMENT_HTML_URL);
  });
});

describe("Forgejo comment journey", () => {
  it("dispatches a verified comment, enqueues, matches, and reacts on the comment", async () => {
    const enqueued: DurableProviderEvent[] = [];
    const posted: string[] = [];
    const consumer = createForgejoCommentConsumer({
      enqueue: async (trigger) => {
        enqueued.push(trigger);
      },
      listTargets: async () => [target("project-a", "rev-1")],
    });
    const observation = await dispatchForgejoClaimed({
      delivery: await verified("issue-comment-created"),
      receiptId: "receipt-journey",
      connection: CONNECTION,
      consumers: { workflow: consumer },
    });
    assert.equal(observation.workflow.status, "succeeded");
    assert.equal(enqueued.length, 1);
    const trigger = enqueued[0];
    if (trigger === undefined) throw new Error("expected enqueued trigger");
    const { project, revision, store } = await createActiveProjectConfiguration(
      createMemoryDatabase(),
      routeConfig("forgejo.issue_comment_created"),
    );
    const provider = createForgejoCommentTriggerProvider({
      configurationStoreForProject: () => store,
      connectionFor: async () => CONNECTION,
      reactions: {
        create: (input) => {
          posted.push(`${input.subject.kind}:${String(input.subject.id)}:${input.content}`);
          return Promise.resolve();
        },
      },
    });
    const matched = await provider.match({
      ...trigger,
      projectId: project.id,
      configurationRevisionId: revision.id,
    });
    if (typeof matched === "string") throw new Error(matched);
    const accepted = matched[0];
    if (accepted === undefined || !isAcceptedTriggerProviderMatch(accepted)) {
      throw new Error("expected accepted journey match");
    }
    assert.deepEqual(accepted.triggerContext.reactionSubject, { kind: "comment", id: 2 });
    await provider.onDispatchAccepted?.(accepted.triggerContext, accepted.outputContext);
    assert.deepEqual(posted, ["comment:2:eyes"]);
  });
});

async function matchNamed(fixture: string, on: string) {
  const { provider, project, revision } = await createProvider(on);
  const delivery = await fixtureDelivery(fixture);
  return provider.match({
    providerEventReceiptId: "receipt-1",
    organizationId: project.organizationId,
    projectId: project.id,
    configurationRevisionId: revision.id,
    source: on,
    deliveryId: String(delivery.headers["x-forgejo-delivery"]),
    receivedAt: new Date("2026-08-30T12:00:00Z"),
    payload: envelope(delivery),
    connectionId: CONNECTION.id,
    resourceId: "1",
  });
}

async function matchWithReactions(fixture: string, posted: string[]) {
  const ready = await createProvider("forgejo.issue_comment_created", posted);
  const delivery = await fixtureDelivery(fixture);
  const match = await ready.provider.match({
    providerEventReceiptId: "receipt-1",
    organizationId: ready.project.organizationId,
    projectId: ready.project.id,
    configurationRevisionId: ready.revision.id,
    source: "forgejo.issue_comment_created",
    deliveryId: String(delivery.headers["x-forgejo-delivery"]),
    receivedAt: new Date("2026-08-30T12:00:00Z"),
    payload: envelope(delivery),
    connectionId: CONNECTION.id,
    resourceId: "1",
  });
  return { provider: ready.provider, match };
}

async function createProvider(on: string, posted?: string[]) {
  const { project, revision, store } = await createActiveProjectConfiguration(
    createMemoryDatabase(),
    routeConfig(on),
  );
  const options: Parameters<typeof createForgejoCommentTriggerProvider>[0] = {
    configurationStoreForProject: () => store,
    connectionFor: async () => CONNECTION,
  };
  if (posted !== undefined) options.reactions = recordingReactions(posted);
  const provider = createForgejoCommentTriggerProvider(options);
  return { provider, project, revision };
}

function recordingReactions(posted: string[]): ForgejoCommentReactionClient {
  return {
    create: (input) => {
      posted.push(`${input.subject.kind}:${String(input.subject.id)}:${input.content}`);
      return Promise.resolve();
    },
  };
}

function routeConfig(on: string) {
  return {
    environments: [{ name: "runner", kind: "daemon", daemon: "runner", cwd: "/repo" }],
    triggers: [
      {
        name: "forgejo-comment",
        on,
        max_runtime: "2h",
        filters: { from_users: ["*"] },
        steps: [
          {
            id: "reply",
            environment: "runner",
            max_runtime: "1h",
            idle_timeout: "5m",
            agent: { provider: "opencode", mode: "default" },
            prompt: [{ text: "Handle the comment" }],
          },
        ],
      },
    ],
  };
}

function target(projectId: string, configurationRevisionId: string): ForgejoCommentDispatchTarget {
  return {
    projectId,
    organizationId: "org-1",
    configurationRevisionId,
    connectionId: CONNECTION.id,
    resourceId: "1",
  };
}

function envelope(delivery: Awaited<ReturnType<typeof fixtureDelivery>>) {
  return {
    headers: {
      "x-forgejo-delivery": delivery.headers["x-forgejo-delivery"],
      "x-forgejo-event": delivery.event,
      "x-forgejo-event-type": delivery.eventType,
    },
    raw: delivery.raw,
  };
}

async function fixtureDelivery(name: string) {
  const fixtures = await loadForgejoContractFixtures();
  return deliveryByName(fixtures, name);
}

async function verified(name: string): Promise<ForgejoVerifiedDelivery> {
  const delivery = await fixtureDelivery(name);
  return {
    connectionId: CONNECTION.id,
    organizationId: "org-1",
    repositoryId: 1,
    deliveryId: String(delivery.headers["x-forgejo-delivery"]),
    event: delivery.event,
    eventType: delivery.eventType,
    signatureHash: "aa".repeat(32),
    rawBody: new TextEncoder().encode(delivery.raw),
    receivedAt: new Date("2026-08-30T12:00:00Z"),
  };
}
