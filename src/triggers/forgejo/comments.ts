import type { Database } from "../../db/types.js";
import type { ProjectConfigurationStore } from "../../configuration/store.js";
import type {
  TriggerHandler,
  TriggerProvider,
  TriggerProviderMatch,
  TriggerProviderReactionState,
  TriggerSource,
} from "../index.js";
import { matchesInputFilters, parseInvocation } from "../invocation.js";
import type { ForgejoWorkflowRunConsumer } from "./dispatch.js";
import { registerForgejoWorkflowRunConsumer } from "./dispatch.js";
import {
  matchForgejoTriggers,
  readForgejoInvocationMessage,
  readForgejoInvocationParserMessage,
  readForgejoMention,
} from "./matching.js";
import {
  normalizeForgejoReceiptPayload,
  type ForgejoConnectionContext,
  type ForgejoNormalizedResult,
  type NormalizedForgejoEvent,
} from "./normalize.js";
import type { ForgejoTriggerContext } from "./provider.js";
import type { ForgejoVerifiedDelivery } from "./webhook.js";
import type {
  ForgejoCommentReactionClient,
  ForgejoCommentReactionContent,
  ForgejoCommentReactionSubject,
} from "./comment-reactions.js";

export const FORGEJO_COMMENT_TRIGGER_EVENT_NAMES = [
  "forgejo.issue_comment",
  "forgejo.issue_comment_created",
  "forgejo.pull_request_comment_created",
] as const;

export interface ForgejoCommentDispatchTarget {
  projectId: string;
  organizationId: string;
  configurationRevisionId: string;
  connectionId: string;
  resourceId: string | null;
}

export interface ForgejoCommentTriggerContext extends ForgejoTriggerContext {
  reactionSubject: ForgejoCommentReactionSubject | null;
}

export function createForgejoCommentConsumer(options: {
  enqueue: TriggerHandler;
  listTargets: (input: {
    organizationId: string;
    connectionId: string;
    repositoryId: number;
  }) => Promise<readonly ForgejoCommentDispatchTarget[]>;
}): ForgejoWorkflowRunConsumer {
  return {
    async consume(input) {
      if (input.event.rawFamily !== "forgejo.issue_comment") return;
      const targets = await options.listTargets({
        organizationId: input.delivery.organizationId,
        connectionId: input.event.context.connectionId,
        repositoryId: input.event.context.repository.id,
      });
      await Promise.all(
        targets.map((target) =>
          options.enqueue(commentTrigger(input.receiptId, input.delivery, input.event, target)),
        ),
      );
    },
  };
}

export function createForgejoCommentSource(options: { database: Database }): TriggerSource {
  return {
    async start(handler: TriggerHandler) {
      registerForgejoWorkflowRunConsumer(
        createForgejoCommentConsumer({
          enqueue: handler,
          listTargets: (input) =>
            options.database.listActiveTriggerDispatchTargets({
              organizationId: input.organizationId,
              provider: "forgejo",
              connectionId: input.connectionId,
              resourceId: String(input.repositoryId),
            }),
        }),
      );
    },
    async stop() {
      return;
    },
  };
}

export function createForgejoCommentTriggerProvider(options: {
  configurationStoreForProject: (projectId: string) => ProjectConfigurationStore;
  connectionFor?: (connectionId: string) => Promise<ForgejoConnectionContext | undefined>;
  reactions?: ForgejoCommentReactionClient;
}): TriggerProvider<"forgejo", ForgejoCommentTriggerContext> {
  return {
    name: "forgejo",
    eventNames: [...FORGEJO_COMMENT_TRIGGER_EVENT_NAMES],
    async match(externalTrigger) {
      const connectionId = externalTrigger.connectionId;
      if (connectionId === undefined || connectionId === null) return "no_trigger_for_source";
      const connection =
        options.connectionFor === undefined
          ? fallbackConnection(connectionId)
          : await options.connectionFor(connectionId);
      if (connection === undefined) return "configuration_unavailable";
      const normalized = normalizeForgejoReceiptPayload({
        payload: externalTrigger.payload,
        connection,
      });
      if (!isCompleteCommentEvent(normalized)) return "no_trigger_for_source";
      const stored = await options
        .configurationStoreForProject(externalTrigger.projectId)
        .getRevision(externalTrigger.configurationRevisionId);
      if (stored === undefined) return "configuration_unavailable";
      const event = normalized.event;
      if (
        !stored.configuration.triggers.some((candidate) =>
          [event.receiptSource, event.rawFamily, event.semanticEvent].includes(candidate.on),
        )
      ) {
        return "no_trigger_for_source";
      }
      return matchCommentTriggers(stored, event, externalTrigger);
    },
    async materializeContext(launch) {
      return launch.triggerContext.event;
    },
    async onDispatchAccepted(triggerContext, _outputContext, reactionState) {
      return projectCommentReaction(options.reactions, triggerContext, "eyes", reactionState);
    },
    async onAgentExecutionCompleted(triggerContext, _outputContext, _result, reactionState) {
      return projectCommentReaction(options.reactions, triggerContext, "+1", reactionState);
    },
    async onAgentExecutionFailed(triggerContext, _outputContext, _reason, reactionState) {
      return projectCommentReaction(options.reactions, triggerContext, "-1", reactionState);
    },
    async onMachineTerminated(triggerContext, _reason, reactionState) {
      return projectCommentReaction(options.reactions, triggerContext, "-1", reactionState);
    },
  };
}

function commentTrigger(
  receiptId: string,
  delivery: ForgejoVerifiedDelivery,
  event: NormalizedForgejoEvent,
  target: ForgejoCommentDispatchTarget,
) {
  return {
    providerEventReceiptId: receiptId,
    organizationId: target.organizationId,
    projectId: target.projectId,
    configurationRevisionId: target.configurationRevisionId,
    source: event.semanticEvent ?? event.rawFamily,
    deliveryId: delivery.deliveryId,
    receivedAt: delivery.receivedAt,
    payload: {
      headers: {
        "x-forgejo-delivery": delivery.deliveryId,
        "x-forgejo-event": delivery.event,
        "x-forgejo-event-type": delivery.eventType,
      },
      raw: new TextDecoder("utf-8").decode(delivery.rawBody),
    },
    connectionId: target.connectionId,
    resourceId: target.resourceId,
  };
}

function isCompleteCommentEvent(
  result: ForgejoNormalizedResult,
): result is { kind: "event"; event: NormalizedForgejoEvent } {
  return result.kind === "event" && result.event.rawFamily === "forgejo.issue_comment";
}

async function matchCommentTriggers(
  stored: NonNullable<Awaited<ReturnType<ProjectConfigurationStore["getRevision"]>>>,
  event: NormalizedForgejoEvent,
  externalTrigger: {
    connectionId?: string | null;
    receivedAt: Date;
  },
) {
  const reactionSubject = commentReactionSubject(event);
  const matches: TriggerProviderMatch<ForgejoCommentTriggerContext>[] = [];
  for (const match of matchForgejoTriggers(
    stored.configuration,
    event,
    externalTrigger.connectionId,
  )) {
    const compiledTrigger = stored.configuration.triggers.find(
      (candidate) => candidate.name === match.trigger.name,
    );
    if (compiledTrigger === undefined) {
      throw new Error(`compiled trigger not found: ${match.trigger.name}`);
    }
    const triggerContext = commentTriggerContext(
      event,
      externalTrigger.receivedAt,
      reactionSubject,
    );
    const invocation = parseInvocation(
      readForgejoInvocationMessage(event),
      compiledTrigger.inputs,
      readForgejoMention(event, compiledTrigger.filters),
      readForgejoInvocationParserMessage(event, compiledTrigger.filters),
    );
    if (invocation.status === "accepted") {
      if (!matchesInputFilters(invocation.inputs, compiledTrigger.filters?.inputs)) continue;
      matches.push({
        triggerName: match.trigger.name,
        triggerContext,
        outputContext: triggerContext,
        configurationRevisionId: stored.revision.id,
        hubConfig: stored.configuration,
        invocation,
      });
      continue;
    }
    matches.push({
      triggerName: match.trigger.name,
      triggerContext,
      outputContext: triggerContext,
      configurationRevisionId: stored.revision.id,
      hubConfig: stored.configuration,
      invocation,
    });
  }
  return matches.length === 0 ? "trigger_filters_rejected" : matches;
}

function commentTriggerContext(
  event: NormalizedForgejoEvent,
  receivedAt: Date,
  reactionSubject: ForgejoCommentReactionSubject | null,
): ForgejoCommentTriggerContext {
  return {
    provider: "forgejo",
    target: {
      connectionId: event.context.connectionId,
      repositoryId: event.context.repository.id,
      repository: event.context.repository.full_name,
    },
    event: {
      forgejo: {
        delivery_id: event.context.deliveryId,
        event_name: event.rawFamily,
        repository: {
          full_name: event.context.repository.full_name,
          id: event.context.repository.id,
        },
        actor: event.context.actor,
        received_at: receivedAt.toISOString(),
        identity: event.identity,
      },
    },
    reactionSubject,
  };
}

function commentReactionSubject(
  event: NormalizedForgejoEvent,
): ForgejoCommentReactionSubject | null {
  const subject = event.context.subject;
  if (subject === null || subject.kind !== "comment") return null;
  const id = numericId(subject.id);
  if (id === undefined) return null;
  return { kind: "comment", id };
}

function numericId(value: number | string): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

async function projectCommentReaction(
  reactions: ForgejoCommentReactionClient | undefined,
  triggerContext: ForgejoCommentTriggerContext,
  content: ForgejoCommentReactionContent,
  _reactionState: TriggerProviderReactionState | undefined,
): Promise<TriggerProviderReactionState> {
  if (reactions === undefined || triggerContext.reactionSubject === null) return null;
  const [owner, repo] = triggerContext.target.repository.split("/");
  if (owner === undefined || repo === undefined) return null;
  await reactions.create({
    connectionId: triggerContext.target.connectionId,
    owner,
    repo,
    subject: triggerContext.reactionSubject,
    content,
  });
  return {
    content,
    kind: triggerContext.reactionSubject.kind,
    id: triggerContext.reactionSubject.id,
  };
}

function fallbackConnection(connectionId: string): ForgejoConnectionContext {
  return { id: connectionId, slug: connectionId, instanceId: connectionId };
}
