"""Syntax-only structural adapter for Python (tree-sitter), PLATFORM-033.

Run as ``python -m agent_context_platform.indexing.tree_sitter.python`` it is the untrusted
child of ``SandboxedAdapter`` (``ParseRequest`` on stdin, ``ParsedModule`` on stdout);
``python_adapter()`` builds that sandboxed adapter. It never opens a file: everything comes
on stdin and the grammar is loaded from site-packages only.

What is emitted
- ``module`` for the file (``__init__.py`` is the package), named from the path;
- ``class`` (nested too), ``function`` and ``method`` (sync and async), ``property`` for
  ``@property``/``@x.setter``/``@cached_property`` methods, module-level ``variable`` and
  ``constant`` (ALL_CAPS), class-level ``field`` and ``type`` for ``type X = ...``;
- ``inherits`` edges (a class to a same-file base class it names) and ``calls`` edges (a
  caller to a same-file function/method/class named at the call site).

Every edge is a *heuristic* structural candidate: ``evidence_kind`` is always
``tree_sitter`` and nothing here resolves scope, imports, aliases, ``obj.method()`` or
dynamic calls. The P032 contract only lets an edge end at a symbol emitted for the same
file, so a call/base/import whose target lives elsewhere (``import os``, ``from x import y``,
``Imported()``) has no representable target and yields no relation. That gap is reported to
the coordinator rather than worked around with placeholder symbols. Per source symbol the
first call site of each target is kept (at most ``MAX_RELATIONS_PER_SYMBOL`` edges).

Semantic fingerprint and signature digest (they feed SymbolRevision identity)
Both hash a *normalized token stream* of the symbol's syntax tree: node types, identifier
and literal text, keywords and operators. Whitespace, comments, line continuations,
commas, quote style and docstrings are not part of it, so reformatting (black: exploded
parameters, trailing commas, quotes) does not create a new revision, while a changed
body, operator, literal or decorator (``@staticmethod`` vs ``@classmethod``) does. The
docstring choice: a changed docstring keeps the same revision because a docstring is
documentation, not behaviour. The signature digest covers decorators and the header up to
the body; the fingerprint covers the whole definition including nested definitions.

Syntax errors. tree-sitter recovers, so the walk never fails. A definition or assignment
that *contains* an ERROR or MISSING node (not counting nested definitions, which decide for
themselves) is skipped together with everything nested inside it, because nested names are
qualified through it. Precisely: an error inside a method's own tokens drops only that method
(the class and its other members are kept), but an ERROR node sitting directly in a class body,
between members, belongs to the class and drops the whole class with its members. Enclosing an
error is not overlapping it. The module symbol is always emitted. Dropped symbols release
their name/output/slot budget, so malformed declarations never starve valid ones.

Work limits (``_common.Budget``): at most ``MAX_NODES_PER_FILE`` (600k) syntax nodes per file (about 3 s of CPU) and a
5 s CPU backstop per request (the runner's RLIMIT_CPU is 10 s); a file over budget degrades to no
symbols while the rest of the batch continues. Call resolution is memoized, a name with more
than 8 same-scope definitions yields no edge, and resolution stops for a source at its
relation cap.

Robustness: the walk is an iterative cursor loop (no recursion on nesting), byte offsets
are those of the raw input (CRLF and any BOM untouched), names must be valid UTF-8 whole
tokens, and per-file limits are enforced adapter-side. A file whose answer the parent would
still refuse degrades to no symbols; the other files of the batch are unaffected.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

import tree_sitter_python
from tree_sitter import Language, Node, Parser, Tree

from agent_context_platform.indexing.tree_sitter._common import (
    Budget,
    WorkBudgetExceeded,
    cut_signature,
    digest,
    frame,
    identifier_bytes,
    module_name,
    safe_module,
)
from agent_context_platform.indexing.tree_sitter.base import (
    EVIDENCE_KIND,
    MAX_NAME_BYTES,
    MAX_RELATIONS_PER_FILE,
    MAX_RELATIONS_PER_SYMBOL,
    MAX_SYMBOLS_PER_FILE,
    ParsedFile,
    ParsedModule,
    ParsedSymbol,
    ParseRequest,
    SourceFile,
    StructuralRelation,
    SymbolKind,
    parser_fingerprint,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits, SandboxedAdapter, serve

LANGUAGE: Final = "python"
ADAPTER_NAME: Final = "agent-context-python-tree-sitter"
# Bump whenever the emitted structure or the fingerprint token stream changes.
ADAPTER_VERSION: Final = "1"
# Pinned in pyproject.toml/uv.lock; a test asserts these equal the installed distributions,
# so the child never reads package metadata at run time.
GRAMMAR_VERSIONS: Final = {"tree-sitter": "0.26.0", "tree-sitter-python": "0.25.0"}
FINGERPRINT: Final = parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS)

_SEMANTIC_DOMAIN: Final = b"agent-context/python/semantic/v1"
_SIGNATURE_DOMAIN: Final = b"agent-context/python/signature/v1"
_CLOSE: Final = frame(b")")
_MAX_CALL_SITES: Final = 200_000
_MAX_CANDIDATES: Final = 8
_DEFINITIONS: Final = frozenset({"function_definition", "class_definition"})
_SKIPPED: Final = frozenset({"comment", "line_continuation", "string_end", ",", ";"})
_PROPERTY_NAMES: Final = frozenset({"property", "cached_property"})
_PROPERTY_ATTRIBUTES: Final = frozenset({"setter", "getter", "deleter", "cached_property"})
_CALLABLE_KINDS: Final = frozenset({"function", "method", "class"})

_PARSER: Final = Parser(Language(tree_sitter_python.language()))


@dataclass(eq=False, slots=True)
class _Sym:
    simple: str
    qualified: str
    kind: SymbolKind
    start: int
    end: int
    parent: _Sym | None
    signature: str = ""
    signature_digest: str = ""
    semantic: str = ""
    alive: bool = True


@dataclass(eq=False, slots=True)
class _Frame:
    """An open definition/assignment: where its tokens start and which symbols it owns."""

    node_id: int
    is_definition: bool
    tok_start: int
    first_sym: int
    syms: list[_Sym] = field(default_factory=list)
    body_id: int = -1
    header_end: int = -1
    tainted: bool = False


@dataclass(frozen=True, slots=True)
class _Site:
    scope: _Sym
    kind: str  # "name": ``f()``; "member": ``self.f()``; "base": a class base
    name: str
    start: int
    end: int


@dataclass(eq=False)
class _Walk:
    content: bytes
    budget: Budget
    output: list[int]
    root: _Sym
    syms: list[_Sym] = field(default_factory=list)
    sites: list[_Site] = field(default_factory=list)
    tokens: list[bytes] = field(default_factory=list)
    frames: list[_Frame] = field(default_factory=list)
    scopes: list[_Sym | None] = field(default_factory=list)
    decorated: dict[int, tuple[int, int]] = field(default_factory=dict)
    # Ancestors as (type, id), kept by the walk itself: ``Node.parent`` is not O(1).
    ancestors: list[tuple[str, int]] = field(default_factory=list)
    first_child: dict[int, int] = field(default_factory=dict)
    seen: set[tuple[str, str, int, int]] = field(default_factory=set)
    property_decorator: bool = False

    def add(self, sym: _Sym, parent: _Frame | None = None) -> bool:
        """Register a symbol if limits allow; the caller drops its subtree otherwise."""
        if len(self.syms) >= MAX_SYMBOLS_PER_FILE:
            return False
        key = (sym.qualified, sym.kind, sym.start, sym.end)
        if key in self.seen:  # ``a = a = 1``, ``a, a = v``: one construct, one symbol
            return False
        if not self.budget.take(sym.qualified, sym.signature, self.output):
            return False
        self.seen.add(key)
        self.syms.append(sym)
        if parent is not None:
            parent.syms.append(sym)
        return True


def _text(content: bytes, node: Node) -> bytes:
    """Source text of a node that is small by construction (a leaf token or a checked name).

    Sliced from the source: a tree parsed through a read callback keeps no copy of it, and
    ``Node.text`` on such a tree is far slower than a slice.
    """
    return content[node.start_byte : node.end_byte]


def _name_bytes(content: bytes, node: Node) -> bytes | None:
    """Text of an identifier-like node, read only when its byte length is name-sized.

    ``Node.text`` copies the whole subtree, so it is never called on a node whose size the
    input controls (a call chain ``a.f().f()...`` nests its whole prefix in each link).
    """
    if node.end_byte - node.start_byte > MAX_NAME_BYTES:
        return None
    return _text(content, node)


def _qualified(parent: _Sym, simple: str) -> str:
    return f"{parent.qualified}.{simple}" if parent.qualified else simple


def _first_identifier(node: Node) -> Node | None:
    """The leftmost identifier of a pattern/type (``T[int]``, ``pkg.T`` -> ``T``/``pkg``)."""
    current: Node | None = node
    while current is not None and current.type != "identifier":
        current = current.named_children[0] if current.named_child_count else None
    return current


def _is_docstring(walk: _Walk, node: Node) -> bool:
    """A lone plain string as the first statement of a module, class or function body."""
    if node.named_child_count != 1 or not walk.ancestors:
        return False
    parent_type, parent_id = walk.ancestors[-1]
    if parent_type != "module" and not (
        parent_type == "block" and len(walk.ancestors) > 1 and walk.ancestors[-2][0] in _DEFINITIONS
    ):
        return False
    if walk.first_child.get(parent_id) != node.id:
        return False
    string = node.named_child(0)
    if string is None or string.type != "string":
        return False
    return not any(child.type == "interpolation" for child in string.children)


def _leaf_token(content: bytes, node: Node) -> bytes:
    if node.type == "string_start":
        # Prefix letters (b, f, r, u) change meaning; the quote style does not.
        prefix = _text(content, node).translate(None, b"'\"").lower()
        return frame(b"s:" + prefix)
    if node.type == "string_content":
        # Python reads any newline sequence in a literal as \n: LF/CRLF conversion is no edit.
        text = _text(content, node).replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        return frame(b"n:string_content=" + text)
    if node.is_named:
        return frame(b"n:" + node.type.encode() + b"=" + _text(content, node))
    return frame(b"a:" + node.type.encode())


def _is_property_decorator(content: bytes, decorator: Node) -> bool:
    target = decorator.named_child(0)
    if target is None:
        return False
    if target.type == "identifier":
        return _name_bytes(content, target) in {n.encode() for n in _PROPERTY_NAMES}
    if target.type == "attribute":
        attribute = target.child_by_field_name("attribute")
        return attribute is not None and _name_bytes(content, attribute) in {
            n.encode() for n in _PROPERTY_ATTRIBUTES
        }
    return False


def _header_end(body: Node) -> int:
    """End of the header (its ``:``), before any comment that precedes the body block."""
    before = body.prev_sibling
    while before is not None and before.type == "comment":
        before = before.prev_sibling
    return before.end_byte if before is not None else body.start_byte


def _enter_definition(walk: _Walk, node: Node) -> None:
    parent_scope = walk.scopes[-1]
    decorated = bool(walk.ancestors) and walk.ancestors[-1][0] == "decorated_definition"
    if decorated:
        start, tok_start = walk.decorated[walk.ancestors[-1][1]]
    else:
        start, tok_start = node.start_byte, len(walk.tokens)
    entry = _Frame(node.id, True, tok_start, len(walk.syms))
    body = node.child_by_field_name("body")
    name_node = node.child_by_field_name("name")
    sym: _Sym | None = None
    if parent_scope is not None and body is not None and name_node is not None:
        entry.body_id = body.id
        raw = _name_bytes(walk.content, name_node)
        simple = (
            identifier_bytes(raw, walk.content, name_node.start_byte, name_node.end_byte)
            if raw is not None
            else None
        )
        if simple is not None:
            kind: SymbolKind
            if node.type == "class_definition":
                kind = "class"
            elif parent_scope.kind == "class":
                kind = "property" if decorated and walk.property_decorator else "method"
            else:
                kind = "function"
            candidate = _Sym(
                simple,
                _qualified(parent_scope, simple),
                kind,
                start,
                node.end_byte,
                parent_scope,
                cut_signature(walk.content[node.start_byte : _header_end(body)]),
            )
            if walk.add(candidate, entry):
                sym = candidate
    walk.frames.append(entry)
    walk.scopes.append(sym)
    if sym is not None and node.type == "class_definition":
        bases = node.child_by_field_name("superclasses")
        for base in bases.named_children if bases is not None else ():
            raw = _name_bytes(walk.content, base) if base.type == "identifier" else None
            if raw is not None:
                walk.sites.append(
                    _Site(sym, "base", raw.decode("utf-8", "replace"), *base.byte_range)
                )


def _assignment_targets(node: Node) -> list[Node]:
    """Flat identifier targets of ``a = b = 1``, ``x: int``, ``a, (b, c) = ...``."""
    found: list[Node] = []
    stack: list[Node] = [node]
    while stack:
        current = stack.pop()
        if current.type == "assignment":
            left = current.child_by_field_name("left")
            right = current.child_by_field_name("right")
            if right is not None and right.type == "assignment":
                stack.append(right)
            if left is not None:
                stack.append(left)
        elif current.type in ("pattern_list", "tuple_pattern", "list_pattern"):
            stack.extend(reversed(current.named_children))
        elif current.type == "identifier":
            found.append(current)
    found.sort(key=lambda item: item.start_byte)
    return found


def _enter_statement(walk: _Walk, node: Node) -> None:
    """Module-level assignments/type aliases and class-level fields."""
    scope = walk.scopes[-1]
    if scope is None or not walk.ancestors:
        return
    parent_type = walk.ancestors[-1][0]
    at_module = parent_type == "module" and scope is walk.root
    in_class = (
        parent_type == "block"
        and len(walk.ancestors) > 1
        and walk.ancestors[-2][0] == "class_definition"
        and scope.kind == "class"
    )
    if not (at_module or in_class):
        return
    entry = _Frame(node.id, False, len(walk.tokens), len(walk.syms))
    if node.type == "type_alias_statement":
        left = node.child_by_field_name("left")
        right = node.child_by_field_name("right")
        name = _first_identifier(left) if left is not None else None
        found = [] if name is None else [name]
        kind: SymbolKind = "type"
        value = right
    else:
        assignment = node.named_child(0)
        if assignment is None or assignment.type != "assignment":
            return
        found = _assignment_targets(assignment)
        kind = "field" if in_class else "variable"
        value = assignment.child_by_field_name("right")
        if value is not None and value.type == "assignment":
            value = None
    end_of_signature = value.start_byte if value is not None else node.end_byte
    signature = cut_signature(walk.content[node.start_byte : end_of_signature])
    for target in found:
        raw = _name_bytes(walk.content, target)
        simple = (
            identifier_bytes(raw, walk.content, target.start_byte, target.end_byte)
            if raw is not None
            else None
        )
        if simple is None:
            continue
        actual: SymbolKind = kind
        if kind == "variable" and simple.isascii() and simple.isupper():
            actual = "constant"
        walk.add(
            _Sym(
                simple,
                _qualified(scope, simple),
                actual,
                node.start_byte,
                node.end_byte,
                scope,
                signature,
            ),
            entry,
        )
    walk.frames.append(entry)


def _enter_call(walk: _Walk, node: Node) -> None:
    scope = walk.scopes[-1]
    function = node.child_by_field_name("function")
    if scope is None or function is None or len(walk.sites) >= _MAX_CALL_SITES:
        return
    if function.type == "identifier":
        raw = _name_bytes(walk.content, function)
        if raw is not None:
            walk.sites.append(
                _Site(scope, "name", raw.decode("utf-8", "replace"), *function.byte_range)
            )
    elif function.type == "attribute":
        owner = function.child_by_field_name("object")
        attribute = function.child_by_field_name("attribute")
        # ``owner`` may be the whole nested prefix of a chain: test its type and size first.
        if owner is None or attribute is None or owner.type != "identifier":
            return
        if owner.end_byte - owner.start_byte not in (3, 4) or _text(walk.content, owner) not in (
            b"self",
            b"cls",
        ):
            return
        raw = _name_bytes(walk.content, attribute)
        if raw is not None:
            walk.sites.append(
                _Site(scope, "member", raw.decode("utf-8", "replace"), *attribute.byte_range)
            )


def _enter(walk: _Walk, node: Node) -> bool:
    """Emit the node's tokens and open frames; True when its children must be walked."""
    kind = node.type
    walk.budget.node()
    if kind in _SKIPPED:
        return False
    if node.is_named and walk.ancestors and walk.ancestors[-1][0] in ("module", "block"):
        walk.first_child.setdefault(walk.ancestors[-1][1], node.id)
    if walk.frames and (node.is_error or node.is_missing):
        walk.frames[-1].tainted = True
    frame_open = walk.frames[-1] if walk.frames else None
    if frame_open is not None and frame_open.body_id == node.id:
        frame_open.header_end = len(walk.tokens)
    if kind == "expression_statement" and _is_docstring(walk, node):
        return False
    if kind == "decorated_definition":
        walk.decorated[node.id] = (node.start_byte, len(walk.tokens))
        walk.property_decorator = False
    elif kind == "decorator":
        walk.property_decorator = walk.property_decorator or _is_property_decorator(
            walk.content, node
        )
    elif kind in _DEFINITIONS:
        _enter_definition(walk, node)
    elif kind in ("expression_statement", "type_alias_statement"):
        _enter_statement(walk, node)
    elif kind == "call":
        _enter_call(walk, node)
    if node.child_count == 0:
        walk.tokens.append(_leaf_token(walk.content, node))
        return False
    walk.tokens.append(frame(b"(" + kind.encode()))
    return True


def _finish_frame(walk: _Walk, entry: _Frame) -> None:
    if entry.tainted:
        # Release what the dropped symbols were charged so they cannot starve valid siblings.
        for sym in walk.syms[entry.first_sym :]:
            sym.alive = False
            walk.budget.release(sym.qualified, sym.signature, walk.output)
        del walk.syms[entry.first_sym :]
        return
    # Definitions: decorators + header up to the body. Assignments: the whole statement.
    stop = entry.header_end if entry.is_definition else len(walk.tokens)
    semantic = digest(_SEMANTIC_DOMAIN, walk.tokens[entry.tok_start :])
    if entry.syms:
        signature = digest(_SIGNATURE_DOMAIN, walk.tokens[entry.tok_start : stop])
        for sym in entry.syms:
            sym.signature_digest = signature
            sym.semantic = semantic
    # Merkle step: an enclosing symbol sees this whole subtree as one token, so every token is
    # hashed once per level it belongs to a *new* subtree, not once per enclosing definition.
    # Enclosing header slices stay valid: nested frames only start after their header ends.
    del walk.tokens[entry.tok_start :]
    walk.tokens.append(frame(b"d:" + semantic.encode()))


def _exit(walk: _Walk, node: Node, opened: bool) -> None:
    if opened:
        walk.tokens.append(_CLOSE)
        walk.ancestors.pop()
    if walk.frames and walk.frames[-1].node_id == node.id:
        entry = walk.frames.pop()
        if entry.is_definition:
            walk.scopes.pop()
        _finish_frame(walk, entry)


def _traverse(tree: Tree, walk: _Walk) -> None:
    cursor = tree.walk()
    while True:
        node = cursor.node
        if node is None:
            return
        if _enter(walk, node) and cursor.goto_first_child():
            walk.ancestors.append((node.type, node.id))
            continue
        opened = False
        while True:
            _exit(walk, node, opened)
            if cursor.goto_next_sibling():
                break
            if not cursor.goto_parent():
                return
            parent = cursor.node
            if parent is None:
                return
            node, opened = parent, True


def _index_members(syms: Sequence[_Sym]) -> dict[tuple[int, str], list[_Sym]]:
    members: dict[tuple[int, str], list[_Sym]] = {}
    for sym in syms:
        if sym.parent is not None:
            members.setdefault((id(sym.parent), sym.simple), []).append(sym)
    return members


def _resolve(
    members: dict[tuple[int, str], list[_Sym]], site: _Site, kinds: frozenset[str]
) -> list[_Sym]:
    """Same-file candidates by name. Heuristic: lexical scope order, no import/alias logic.

    A name with more than ``_MAX_CANDIDATES`` definitions in the scope that binds it is
    ambiguous noise: it yields no edge (and bounds the work per call site).
    """
    if site.kind == "member":
        scope: _Sym | None = site.scope
        while scope is not None and scope.kind != "class":
            scope = scope.parent
        found = members.get((id(scope), site.name), []) if scope is not None else []
        found = [sym for sym in found if sym.kind == "method"]
        return found if len(found) <= _MAX_CANDIDATES else []
    scope = site.scope.parent if site.kind == "base" else site.scope
    first = True
    while scope is not None:
        # Names of an enclosing class body are not visible from its methods.
        if scope.kind != "class" or first:
            found = [s for s in members.get((id(scope), site.name), []) if s.kind in kinds]
            if len(found) > _MAX_CANDIDATES:
                return []
            if found:
                return found
        first = False
        scope = scope.parent
    return []


def _relations(
    syms: list[_Sym], sites: list[_Site], index: dict[int, int]
) -> tuple[StructuralRelation, ...]:
    members = _index_members(syms)
    per_source: dict[int, int] = {}
    seen: set[tuple[int, int, str]] = set()
    memo: dict[tuple[int, str, str], list[_Sym]] = {}
    out: list[StructuralRelation] = []
    ordered = sorted(sites, key=lambda site: site.kind != "base")  # stable: bases first
    for site in ordered:
        source = index.get(id(site.scope))
        if source is None or per_source.get(source, 0) >= MAX_RELATIONS_PER_SYMBOL:
            continue  # unemitted source, or its relation cap is reached: no resolving at all
        base = site.kind == "base"
        kinds = frozenset({"class"}) if base else _CALLABLE_KINDS
        cache_key = (id(site.scope), site.kind, site.name)
        if cache_key not in memo:
            memo[cache_key] = _resolve(members, site, kinds)
        for target in memo[cache_key]:
            key = (source, index[id(target)], "inherits" if base else "calls")
            if key in seen or per_source.get(source, 0) >= MAX_RELATIONS_PER_SYMBOL:
                continue
            if len(out) >= MAX_RELATIONS_PER_FILE:
                return tuple(out)
            seen.add(key)
            per_source[source] = per_source.get(source, 0) + 1
            out.append(
                StructuralRelation(
                    source_ref=str(source),
                    target_ref=str(key[1]),
                    kind="inherits" if base else "calls",
                    start_byte=site.start,
                    end_byte=site.end,
                    evidence_kind=EVIDENCE_KIND,
                )
            )
    return tuple(out)


def _bounded_parse(content: bytes, budget: Budget) -> Tree:
    """Parse under the CPU backstop: checked before, and during (progress callback)."""
    budget.check()

    def read(offset: int, _point: object) -> bytes:
        return content[offset : offset + 65536]

    try:
        return _PARSER.parse(
            read, encoding="utf8", progress_callback=lambda *_: budget.out_of_time()
        )
    except ValueError:  # the callback cancelled the parse
        raise WorkBudgetExceeded from None


def _parse_file(source: SourceFile, budget: Budget, output: list[int]) -> ParsedFile:
    content = source.content()
    empty = ParsedFile(
        path=source.path, language=source.language, parser_fingerprint=FINGERPRINT, symbols=()
    )
    if not content:
        return empty
    name = module_name(source.path)
    root = _Sym(name.rsplit(".", 1)[-1], name, "module", 0, len(content), None)
    walk = _Walk(content, budget, output, root)
    if name and not walk.add(root):
        return empty
    walk.scopes.append(root)
    _traverse(_bounded_parse(content, budget), walk)
    if name:
        root.semantic = digest(_SEMANTIC_DOMAIN, walk.tokens)
        root.signature_digest = digest(_SIGNATURE_DOMAIN, ())
    live = [sym for sym in walk.syms if sym.alive]
    index = {id(sym): number for number, sym in enumerate(live)}
    symbols = tuple(
        ParsedSymbol(
            ref=str(number),
            language=source.language,
            qualified_name=sym.qualified,
            kind=sym.kind,
            start_byte=sym.start,
            end_byte=sym.end,
            signature=sym.signature,
            signature_digest=sym.signature_digest,
            semantic_fingerprint=sym.semantic,
            evidence_kind=EVIDENCE_KIND,
        )
        for number, sym in enumerate(live)
    )
    return ParsedFile(
        path=source.path,
        language=source.language,
        parser_fingerprint=FINGERPRINT,
        symbols=symbols,
        relations=_relations(live, walk.sites, index),
    )


def parse_python(request: ParseRequest) -> ParsedModule:
    """Child-side entry: one ``ParsedFile`` per input file; the parent validates the result."""
    return safe_module(request, FINGERPRINT, _parse_file)


def python_adapter(
    *, limits: Limits | None = None, env: dict[str, str] | None = None
) -> SandboxedAdapter:
    """The sandboxed adapter: this module run as an rlimit-bound child of ``sys.executable``."""
    return SandboxedAdapter(
        [sys.executable, "-m", __name__],
        language=LANGUAGE,
        parser_name=ADAPTER_NAME,
        parser_version=ADAPTER_VERSION,
        parser_config=GRAMMAR_VERSIONS,
        limits=limits,
        env=env,
    )


if __name__ == "__main__":
    sys.exit(serve(parse_python))
