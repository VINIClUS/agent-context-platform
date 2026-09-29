"""One coding session across a hook and an OTel surface, as sealed SDK events."""

from __future__ import annotations

from agent_context_sdk import StoredEventV1

from .conftest import OID_A, OID_B, OID_TREE, SHA, build_event

CONTEXT = {
    "workspace_id": "ws_1",
    "project_id": "prj_1",
    "repository_id": "repo_1",
    "checkout_id": "co_1",
    "session_id": "sess_1",
}
TURN_CONTEXT = {**CONTEXT, "turn_id": "turn_1"}
CALL_CONTEXT = {**TURN_CONTEXT, "tool_call_id": "call_1"}
IDS = {"session_id": "sess_1", "turn_id": "turn_1"}
CALL = {**IDS, "tool_call_id": "call_1", "tool_name": "Bash"}


def session_events() -> list[StoredEventV1]:
    """Thirteen events in causal order; hook events are 4, 6, 8 and OTel are 7, 9."""
    return [
        build_event(
            1,
            "git.repository.observed",
            {
                "repository_id": "repo_1",
                "object_format": "sha1",
                "remote_identities": ["github.com/acme/app"],
            },
            context={"workspace_id": "ws_1", "project_id": "prj_1", "repository_id": "repo_1"},
        ),
        build_event(
            2,
            "git.checkout.observed",
            {
                "repository_id": "repo_1",
                "checkout_id": "co_1",
                "object_format": "sha1",
                "head_commit": OID_A,
                "branch": "main",
                "detached": False,
            },
            context=CONTEXT,
        ),
        build_event(
            3,
            "agent.session.started",
            {
                "source": "codex_hook",
                "session_id": "sess_1",
                "model": "gpt-5",
                "start_reason": "startup",
            },
            context=CONTEXT,
        ),
        build_event(
            4,
            "agent.turn.started",
            {"source": "codex_hook", **IDS, "user_message_content_id": "msg_user_1"},
            context=TURN_CONTEXT,
        ),
        build_event(
            5,
            "agent.tool_call.started",
            {"source": "codex_hook", **CALL, "input_content_id": "in_1"},
            context=CALL_CONTEXT,
        ),
        build_event(
            6,
            "agent.tool_call.output_observed",
            {
                "source": "codex_hook",
                **CALL,
                "input_content_id": "in_1",
                "output_content_id": "out_1",
            },
            context=CALL_CONTEXT,
        ),
        build_event(
            7,
            "agent.tool_call.completed",
            {
                "source": "codex_otel",
                **CALL,
                "success": True,
                "input_content_id": "in_1",
                "output_content_id": "out_1",
                "duration_ms": 120,
            },
            context=CALL_CONTEXT,
        ),
        build_event(
            8,
            "agent.turn.stopped",
            {"source": "codex_hook", **IDS, "stop_hook_active": False},
            context=TURN_CONTEXT,
        ),
        build_event(
            9,
            "agent.turn.completed",
            {
                "source": "codex_otel",
                **IDS,
                "success": False,
                "error_class": "ToolError",
                "assistant_message_content_id": "msg_asst_1",
                "duration_ms": 900,
            },
            context=TURN_CONTEXT,
        ),
        build_event(
            10,
            "git.commit.observed",
            {
                "repository_id": "repo_1",
                "checkout_id": "co_1",
                "commit_id": OID_B,
                "tree_id": OID_TREE,
                "parent_commit_ids": [OID_A],
                "authored_at": "2026-08-13T13:00:10Z",
                "committed_at": "2026-08-13T13:00:11Z",
                "message_content_id": "msg_commit_1",
            },
            context=CONTEXT,
        ),
        build_event(
            11,
            "git.checkout.observed",
            {
                "repository_id": "repo_1",
                "checkout_id": "co_1",
                "object_format": "sha1",
                "head_commit": OID_B,
                "branch": "main",
                "detached": False,
            },
            context=CONTEXT,
        ),
        build_event(
            12,
            "git.workspace_snapshot.captured",
            {
                "snapshot_id": "snap_1",
                "repository_id": "repo_1",
                "checkout_id": "co_1",
                "base_commit": OID_B,
                "dirty_patch_sha256": SHA,
                "modified_content_sha256": [SHA],
                "untracked_paths": ["notes.md"],
            },
            context=CONTEXT,
        ),
        build_event(
            13,
            "agent.session.ended",
            {
                "source": "codex_hook",
                "session_id": "sess_1",
                "end_reason": "exit",
                "duration_ms": 1000,
            },
            context=CONTEXT,
        ),
    ]
