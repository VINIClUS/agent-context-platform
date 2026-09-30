"""Syntax-only structural adapter for TypeScript and JavaScript (tree-sitter), PLATFORM-034.

Run as ``python -m agent_context_platform.indexing.tree_sitter.typescript`` it is the untrusted
child of ``SandboxedAdapter`` (``ParseRequest`` on stdin, ``ParsedModule`` on stdout);
``typescript_adapter()`` builds that sandboxed adapter. It never opens a file: everything comes
on stdin and the grammars are loaded from site-packages only.

Language routing. One adapter, one ``language`` value (``typescript``): the runner requires a
request to carry a single language. The grammar is chosen per file from its extension:
``.ts``/``.mts``/``.cts`` (and unknown) use the TypeScript grammar, ``.tsx`` the TSX grammar and
``.js``/``.jsx``/``.mjs``/``.cjs`` the JavaScript grammar (which parses JSX). All three
distributions are pinned and part of the parser fingerprint.

What is emitted
- ``module`` for the file (``index.*`` is the directory module), named from the path, plus one
  ``module`` per ``namespace`` segment (``namespace A.B`` gives ``A`` and ``A.B``);
- ``class`` (also abstract and ``const C = class {}``), ``interface``, ``type`` (type aliases,
  and enums: the contract has no ``enum`` kind, so an enum is the type it declares), ``function``
  (declarations, and arrow/function expressions bound to a variable), ``method`` (also arrow
  class fields), ``property`` (``get``/``set`` accessors), ``field`` (class fields) and
  module-level ``constant`` (``const``) and ``variable`` (``let``/``var``, identifier names only);
- ``inherits`` edges (``extends``/``implements`` to a same-file class or interface) and ``calls``
  edges (a caller to a same-file function/method/class named at the call site).

References (``ParsedFile.references``), all unresolved names, never resolved symbols
- ``import`` (confidence ``syntactic``): ESM ``import``, ``export ... from``, ``export * from`` and
  ``import x = require("m")``. A module specifier is a string literal; the qualifier is the
  specifier path (its range is the literal's content), ``./`` gives ``relative_level`` 1 and each
  ``../`` one more, a bare specifier (``pkg/sub``, ``@scope/pkg``) has level 0. A named import
  (``{a as b}``, ``export {a} from``) is ``qualifier=<specifier>`` + ``target_name=a``. A
  whole-module form (default, namespace, side-effect, ``export *``, ``import = require``) has no
  imported name: with one specifier segment it is the target itself, with two the first is the
  qualifier and the second the target; with more than two it cannot be said in protocol 2 (a
  qualifier that is a whole literal must come with a name) and is reported as
  ``references_capped``, as are specifiers that are not dotted identifier paths (``lodash-es``,
  ``node:fs``, ``./my-file``: a ``-`` or ``:`` is no separator the contract accepts);
- ``call`` (confidence ``heuristic``): a call or ``new`` whose callee is a name or a plain
  ``a.b.c`` chain that no same-file symbol answers (``require("x")`` is the name ``require``);
- ``inherit`` (``syntactic``): an ``extends``/``implements`` target that no same-file class or
  interface answers.

Every edge and reference is a *heuristic* structural candidate: ``evidence_kind`` is always
``tree_sitter`` and nothing here resolves scope, imports, aliases, ``obj.method()`` on a value or
dynamic calls; a name with more than 8 same-scope candidates yields no edge and no reference. A
call inside an anonymous function belongs to the enclosing named definition.

Semantic fingerprint and signature digest (they feed SymbolRevision identity)
Both hash a *normalized token stream* of the symbol's syntax tree: node types, identifier and
literal text. Comments (JSDoc included), ``;`` and ``,`` (ASI, trailing commas) and the quote
style of string literals are not part of it and CRLF inside string/template text is read as LF,
so reformatting does not create a new revision while a changed body, operator, literal or
decorator does. The signature digest covers the header up to the body; the fingerprint covers the
whole definition including nested definitions.

Syntax errors. tree-sitter recovers, so the walk never fails. A definition that *contains* an
ERROR or MISSING node (not counting nested definitions, which decide for themselves) is skipped
together with everything nested inside it (``symbols_dropped``); the recovery is reported as
``syntax_recovered``. Dropped symbols release their name/output budget.

Work limits (``_common.Budget``): 600k syntax nodes per file and a 5 s CPU backstop per request
(the runner's RLIMIT_CPU is 10 s), enforced inside the parse by ``_common.bounded_parse``. A file
over budget carries ``file_degraded`` + ``work_budget_exceeded`` and no structure; the rest of the
batch continues.

Robustness: the walk is an iterative cursor loop (no recursion on nesting), no ``Node.text`` and no
``Node.parent`` (text is sliced from the source bytes, ancestors are tracked by the walk), names
must be valid UTF-8 whole tokens, byte offsets are those of the raw input (CRLF and BOM untouched).
"""

from __future__ import annotations

import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import cache
from typing import Final, Literal

from tree_sitter import Language, Node, Parser, Tree

from agent_context_platform.indexing.tree_sitter._common import (
    Budget,
    WorkBudgetExceeded,
    bounded_parse,
    cut_signature,
    digest,
    dotted_name,
    frame,
    identifier_bytes,
    module_name,
    safe_module,
)
from agent_context_platform.indexing.tree_sitter.base import (
    EVIDENCE_KIND,
    MAX_DIAGNOSTIC_COUNT,
    MAX_MODULE_REFERENCES,
    MAX_NAME_BYTES,
    MAX_NAME_TOTAL_BYTES,
    MAX_REFERENCES_PER_FILE,
    MAX_REFERENCES_PER_SYMBOL,
    MAX_RELATIONS_PER_FILE,
    MAX_RELATIONS_PER_SYMBOL,
    MAX_RELATIVE_LEVEL,
    MAX_SYMBOLS_PER_FILE,
    ParsedDiagnostic,
    ParsedFile,
    ParsedModule,
    ParsedReference,
    ParsedSymbol,
    ParseRequest,
    SourceFile,
    StructuralRelation,
    SymbolKind,
    parser_fingerprint,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits, SandboxedAdapter, serve

LANGUAGE: Final = "typescript"
ADAPTER_NAME: Final = "agent-context-typescript-tree-sitter"
# Bump whenever the emitted structure or the fingerprint token stream changes.
ADAPTER_VERSION: Final = "1"
# Pinned in pyproject.toml/uv.lock; a test asserts these equal the installed distributions,
# so the child never reads package metadata at run time.
GRAMMAR_VERSIONS: Final = {
    "tree-sitter": "0.26.0",
    "tree-sitter-javascript": "0.25.0",
    "tree-sitter-typescript": "0.23.2",
}
FINGERPRINT: Final = parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS)

_SEMANTIC_DOMAIN: Final = b"agent-context/typescript/semantic/v1"
_SIGNATURE_DOMAIN: Final = b"agent-context/typescript/signature/v1"
_CLOSE: Final = frame(b")")
_MAX_SITES: Final = 200_000
_MAX_CANDIDATES: Final = 8
# A signature is cut at 256 bytes after whitespace collapse; never collapse more than this.
_HEADER_SLICE: Final = 2048
_REFERENCE_NAME_LIMIT: Final = MAX_NAME_TOTAL_BYTES // 2 - 8192

_FUNCTION_DECLARATIONS: Final = frozenset(
    {"function_declaration", "generator_function_declaration", "function_signature"}
)
_CLASS_DECLARATIONS: Final = frozenset({"class_declaration", "abstract_class_declaration"})
_NAMESPACES: Final = frozenset({"internal_module", "module"})
_MEMBERS: Final = frozenset(
    {
        "method_definition",
        "abstract_method_signature",
        "public_field_definition",
        "field_definition",
    }
)
_VARIABLES: Final = frozenset({"lexical_declaration", "variable_declaration"})
_FUNCTION_VALUES: Final = frozenset(
    {"arrow_function", "function_expression", "function", "generator_function"}
)
_WRAPPERS: Final = frozenset({"export_statement", "ambient_declaration"})
_NAME_TYPES: Final = frozenset({"identifier", "type_identifier", "property_identifier"})
_BASE_TYPES: Final = frozenset(
    {"identifier", "type_identifier", "member_expression", "nested_type_identifier"}
)
_HERITAGE_GROUPS: Final = frozenset({"extends_clause", "implements_clause", "extends_type_clause"})
_SKIPPED: Final = frozenset({"comment", ";", ","})
_QUOTES: Final = frozenset({'"', "'"})
_TEXT_LEAVES: Final = frozenset({"string_fragment", "template_chars"})
_CALLABLE_KINDS: Final = frozenset({"function", "method", "class"})
_TYPE_KINDS: Final = frozenset({"class", "interface"})
_SUFFIXES: Final = (
    ".d.ts",
    ".d.mts",
    ".d.cts",
    ".tsx",
    ".ts",
    ".mts",
    ".cts",
    ".jsx",
    ".js",
    ".mjs",
    ".cjs",
)
_JAVASCRIPT: Final = frozenset({"js", "jsx", "mjs", "cjs"})
_IDENT: Final = re.compile(rb"[A-Za-z_$\x80-\xff][A-Za-z0-9_$\x80-\xff]*")
_SPECIFIER: Final = re.compile(
    rb"@?[A-Za-z_$\x80-\xff][A-Za-z0-9_$\x80-\xff]*(?:[/.][A-Za-z_$\x80-\xff][A-Za-z0-9_$\x80-\xff]*)*"
)


@cache
def _parser(flavor: str) -> Parser:
    """One parser per grammar actually used: start-up pays only for what the batch needs."""
    if flavor == "javascript":
        import tree_sitter_javascript

        return Parser(Language(tree_sitter_javascript.language()))
    import tree_sitter_typescript

    raw = (
        tree_sitter_typescript.language_tsx()
        if flavor == "tsx"
        else tree_sitter_typescript.language_typescript()
    )
    return Parser(Language(raw))


def _flavor(path: str) -> str:
    extension = path.rsplit(".", 1)[-1].lower() if "." in path.rsplit("/", 1)[-1] else ""
    if extension in _JAVASCRIPT:
        return "javascript"
    return "tsx" if extension == "tsx" else "typescript"


def _module_name(path: str) -> str:
    """Dotted module name from the path: ``src/a/b.service.ts`` -> ``src.a.b.service``.

    The extension goes, dots inside the file stem separate like directories (``b.service``) and
    ``index`` is the directory itself. The trailing run of identifier components is the name, so
    a component such as ``user-service`` ends the run (empty when the file itself is one).
    """
    directory, _, stem = path.rpartition("/")
    for suffix in _SUFFIXES:
        if stem.endswith(suffix) and len(stem) > len(suffix):
            stem = stem[: -len(suffix)]
            break
    else:
        stem = stem.rsplit(".", 1)[0] if "." in stem else stem
    rebuilt = "/".join(part for part in (directory, stem.replace(".", "/")) if part) + ".ts"
    return module_name(rebuilt, package_files=("index",))


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
    """An open definition or declaration: where its tokens start and which symbols it owns."""

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
    kind: (
        str  # "name": ``f()``; "chain": ``a.b()``; "member": ``this.f()``; "base": a heritage name
    )
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
    # Ancestors as (type, id, start_byte), kept by the walk itself: ``Node.parent`` is not O(1).
    ancestors: list[tuple[str, int, int]] = field(default_factory=list)
    scope_nodes: dict[int, int] = field(default_factory=dict)
    value_scopes: dict[int, _Sym] = field(default_factory=dict)
    seen: set[tuple[str, str, int, int]] = field(default_factory=set)
    seen_sites: set[tuple[int, str, str]] = field(default_factory=set)
    imports: list[ParsedReference] = field(default_factory=list)
    seen_imports: set[tuple[int, int, int]] = field(default_factory=set)
    reference_bytes: int = 0
    decorators_at: int = -1
    recovered: int = 0
    dropped: int = 0
    capped: int = 0

    def add(self, sym: _Sym, parent: _Frame) -> bool:
        """Register a symbol if limits allow; the caller drops its subtree otherwise."""
        if len(self.syms) >= MAX_SYMBOLS_PER_FILE:
            self.dropped += 1
            return False
        key = (sym.qualified, sym.kind, sym.start, sym.end)
        if key in self.seen:  # ``var a = 1, a = 2``: one construct, one symbol
            return False
        if not self.budget.take(sym.qualified, sym.signature, self.output):
            self.dropped += 1
            return False
        self.seen.add(key)
        self.syms.append(sym)
        parent.syms.append(sym)
        return True


def _name_bytes(content: bytes, node: Node) -> bytes | None:
    """Text of an identifier-like node, read only when its byte length is name-sized.

    ``Node.text`` copies the whole subtree, so it is never called; a call chain
    ``a.f().f()...`` nests its whole prefix in each link, so size is checked first.
    """
    if node.end_byte - node.start_byte > MAX_NAME_BYTES:
        return None
    return content[node.start_byte : node.end_byte]


def _identifier(walk: _Walk, node: Node | None) -> str | None:
    """The name of a plain identifier node, or None when the parent would not accept it."""
    if node is None or node.type not in _NAME_TYPES:
        return None
    raw = _name_bytes(walk.content, node)
    if raw is None:
        return None
    return identifier_bytes(raw, walk.content, node.start_byte, node.end_byte)


def _qualified(parent: _Sym, simple: str) -> str:
    return f"{parent.qualified}.{simple}" if parent.qualified else simple


def _outer_start(walk: _Walk, node: Node) -> int:
    """Start of the declaration including ``export``/``declare`` (and decorators before them)."""
    start = node.start_byte
    index = len(walk.ancestors) - 1
    while index >= 0 and walk.ancestors[index][0] in _WRAPPERS:
        start = walk.ancestors[index][2]
        index -= 1
    return start


def _at_top(walk: _Walk) -> bool:
    """A statement of the file or of a namespace body (not of a function or a class)."""
    scope = walk.scopes[-1]
    if scope is None or not (scope is walk.root or scope.kind == "module"):
        return False
    index = len(walk.ancestors) - 1
    while index >= 0 and walk.ancestors[index][0] in _WRAPPERS:
        index -= 1
    if index < 0:
        return False
    if walk.ancestors[index][0] == "program":
        return True
    return (
        walk.ancestors[index][0] == "statement_block"
        and index > 0
        and walk.ancestors[index - 1][0] in _NAMESPACES
    )


def _header(walk: _Walk, start: int, header_end: int) -> str:
    return cut_signature(walk.content[start : min(header_end, start + _HEADER_SLICE)])


def _push(walk: _Walk, node: Node, syms: Sequence[_Sym | None]) -> None:
    walk.scopes.extend(syms)
    if syms:
        walk.scope_nodes[node.id] = len(syms)


def _declare(
    walk: _Walk,
    node: Node,
    names: Sequence[tuple[SymbolKind, str]],
    header_end: int,
    body: Node | None,
) -> list[_Sym]:
    """Open a definition frame for ``names`` (one symbol each, sharing the node's range)."""
    parent = walk.scopes[-1]
    if parent is None:
        return []
    start = _outer_start(walk, node)
    entry = _Frame(node.id, True, len(walk.tokens), len(walk.syms))
    if body is not None:
        entry.body_id = body.id
    signature = _header(walk, start, header_end)
    made: list[_Sym] = []
    scope = parent
    for kind, simple in names:
        candidate = _Sym(
            simple, _qualified(scope, simple), kind, start, node.end_byte, scope, signature
        )
        if not walk.add(candidate, entry):
            break
        made.append(candidate)
        scope = candidate
    if not made:
        _push(walk, node, [None])
        return []
    walk.frames.append(entry)
    _push(walk, node, made)
    return made


def _enter_named(walk: _Walk, node: Node, kind: SymbolKind) -> None:
    name = _identifier(walk, node.child_by_field_name("name"))
    body = node.child_by_field_name("body")
    end = body.start_byte if body is not None else node.end_byte
    if name is None:
        if walk.scopes[-1] is not None and node.type != "type_alias_declaration":
            _push(walk, node, [None])
        return
    made = _declare(walk, node, [(kind, name)], end, body)
    if made and kind in _TYPE_KINDS:
        _heritage(walk, made[0], node)


def _enter_namespace(walk: _Walk, node: Node) -> None:
    if not node.is_named or walk.scopes[-1] is None:
        return
    name = node.child_by_field_name("name")
    body = node.child_by_field_name("body")
    parts: list[str] = []
    stack: list[Node | None] = [name] if name is not None else []
    while stack:  # ``A.B.C`` is a left-nested chain of ``nested_identifier``
        current = stack.pop()
        if current is None:
            parts = []
            break
        if current.type == "nested_identifier":
            stack.append(current.child_by_field_name("property"))
            stack.append(current.child_by_field_name("object"))
            continue
        simple = _identifier(walk, current)
        if simple is None:
            parts = []
            break
        parts.append(simple)
    if not parts or name is None or body is None:
        _push(walk, node, [None])
        return
    _declare(walk, node, [("module", part) for part in parts], body.start_byte, body)


def _member_name(walk: _Walk, node: Node) -> str | None:
    name = node.child_by_field_name("name") or node.child_by_field_name("property")
    return _identifier(walk, name) if name is not None and name.type != "identifier" else None


def _is_accessor(node: Node) -> bool:
    for child in node.children:
        if child.type in ("get", "set") and not child.is_named:
            return True
        if child.type == "property_identifier":
            return False
    return False


def _enter_member(walk: _Walk, node: Node) -> None:
    scope = walk.scopes[-1]
    if scope is None or scope.kind != "class":
        return
    if not walk.ancestors or walk.ancestors[-1][0] != "class_body":
        return
    simple = _member_name(walk, node)
    if simple is None:
        _push(walk, node, [None])
        return
    kind: SymbolKind
    body = node.child_by_field_name("body")
    value: Node | None = None
    if node.type in ("method_definition", "abstract_method_signature"):
        kind = "property" if _is_accessor(node) else "method"
        end = body.start_byte if body is not None else node.end_byte
    else:
        value = node.child_by_field_name("value")
        function = value is not None and value.type in _FUNCTION_VALUES
        kind = "method" if function else "field"
        body = value.child_by_field_name("body") if function and value is not None else None
        end = (
            body.start_byte if body is not None else (value.start_byte if value else node.end_byte)
        )
    made = _declare(walk, node, [(kind, simple)], end, body)
    if made and value is not None and value.type in _FUNCTION_VALUES:
        walk.value_scopes[value.id] = made[0]
    elif made and node.type in ("public_field_definition", "field_definition"):
        walk.frames[-1].is_definition = False


def _enter_variables(walk: _Walk, node: Node) -> None:
    scope = walk.scopes[-1]
    if scope is None:
        return
    top = _at_top(walk)
    marker = node.child_by_field_name("kind")
    constant = marker is not None and marker.type == "const"
    start = _outer_start(walk, node)
    entry = _Frame(node.id, False, len(walk.tokens), len(walk.syms))
    header_end = -1
    for child in node.named_children:
        if child.type != "variable_declarator":
            continue
        value = child.child_by_field_name("value")
        callable_value = value is not None and value.type in _FUNCTION_VALUES
        class_value = value is not None and value.type == "class"
        if not (top or callable_value or class_value):
            continue
        simple = _identifier(walk, child.child_by_field_name("name"))
        if simple is None or (child.child_by_field_name("name") or child).type != "identifier":
            continue
        kind: SymbolKind = (
            "function"
            if callable_value
            else "class"
            if class_value
            else "constant"
            if constant
            else "variable"
        )
        body = value.child_by_field_name("body") if callable_value and value is not None else None
        if header_end < 0:
            header_end = (
                body.start_byte
                if body is not None
                else value.start_byte
                if value is not None
                else node.end_byte
            )
            if body is not None:
                entry.body_id = body.id
                entry.is_definition = True
        candidate = _Sym(
            simple,
            _qualified(scope, simple),
            kind,
            start,
            node.end_byte,
            scope,
            _header(walk, start, header_end),
        )
        if walk.add(candidate, entry) and value is not None and (callable_value or class_value):
            walk.value_scopes[value.id] = candidate
            if class_value and value is not None:
                _heritage(walk, candidate, value)
    if entry.syms:
        walk.frames.append(entry)


def _heritage(walk: _Walk, sym: _Sym, node: Node) -> None:
    """Base sites of a class or interface: every ``extends``/``implements`` name."""
    for child in node.children:
        if child.type == "class_heritage":
            groups = child.named_children
        elif child.type == "extends_type_clause":
            groups = [child]
        else:
            continue
        for group in groups:
            items = group.named_children if group.type in _HERITAGE_GROUPS else [group]
            for item in items:
                target = item
                if target.type == "generic_type":
                    target = target.child_by_field_name("name") or target
                if target.type not in _BASE_TYPES:
                    continue
                name = dotted_name(walk.content, target.start_byte, target.end_byte)
                if name is not None and len(walk.sites) < _MAX_SITES:
                    _add_site(walk, _Site(sym, "base", name, target.start_byte, target.end_byte))


def _add_site(walk: _Walk, site: _Site) -> None:
    key = (id(site.scope), site.kind, site.name)
    if key not in walk.seen_sites:
        walk.seen_sites.add(key)
        walk.sites.append(site)


def _enter_call(walk: _Walk, node: Node) -> None:
    scope = walk.scopes[-1]
    if scope is None or len(walk.sites) >= _MAX_SITES:
        return
    callee = node.child_by_field_name(
        "function" if node.type == "call_expression" else "constructor"
    )
    if callee is None or callee.type not in ("identifier", "member_expression"):
        return
    if callee.end_byte - callee.start_byte > MAX_NAME_BYTES:
        return  # the prefix of a long call chain: never inspected, so a chain costs O(1) per link
    if callee.type == "member_expression" and node.type == "call_expression":
        owner = callee.child_by_field_name("object")
        prop = callee.child_by_field_name("property")
        if owner is not None and owner.type == "this":
            simple = _identifier(walk, prop) if prop is not None else None
            if simple is not None and prop is not None:
                _add_site(walk, _Site(scope, "member", simple, prop.start_byte, prop.end_byte))
            return
    name = dotted_name(walk.content, callee.start_byte, callee.end_byte)
    if name is None or name.split(".", 1)[0] in ("this", "super"):
        return
    _add_site(walk, _Site(scope, "name" if "." not in name else "chain", name, *callee.byte_range))


def _reference(
    walk: _Walk, name: str, start: int, end: int, qualifier: tuple[str, int, int] | None, level: int
) -> None:
    """Module-level import reference; counted against the caps like every other reference."""
    key = (start, end, -1 if qualifier is None else qualifier[1])
    size = len(name.encode()) + (0 if qualifier is None else len(qualifier[0].encode()))
    if key in walk.seen_imports:
        return
    if (
        len(walk.imports) >= MAX_MODULE_REFERENCES
        or walk.reference_bytes + size > _REFERENCE_NAME_LIMIT
    ):
        walk.capped += 1
        return
    walk.seen_imports.add(key)
    walk.reference_bytes += size
    walk.imports.append(
        ParsedReference(
            source=None,
            kind="import",
            target_name=name,
            relative_level=level,
            start_byte=start,
            end_byte=end,
            qualifier=None if qualifier is None else qualifier[0],
            qualifier_start_byte=None if qualifier is None else qualifier[1],
            qualifier_end_byte=None if qualifier is None else qualifier[2],
            evidence_kind=EVIDENCE_KIND,
            confidence="syntactic",
        )
    )


@dataclass(frozen=True, slots=True)
class _Specifier:
    """A module specifier literal: relative level, the literal's content range and its tokens."""

    level: int
    start: int
    end: int
    tokens: tuple[tuple[str, int, int], ...]


def _specifier(walk: _Walk, literal: Node | None) -> _Specifier | None:
    if literal is None or literal.type != "string" or literal.named_child_count != 1:
        return None
    fragment = literal.named_child(0)
    if fragment is None or fragment.type != "string_fragment":
        return None
    if fragment.end_byte - fragment.start_byte > MAX_NAME_BYTES:
        return None
    if walk.content[literal.start_byte : literal.start_byte + 1] not in (b'"', b"'"):
        return None
    raw = walk.content[fragment.start_byte : fragment.end_byte]
    level, rest = 0, raw
    while True:
        if rest.startswith(b"../"):
            level, rest = max(level, 1) + 1, rest[3:]
        elif rest.startswith(b"./"):
            level, rest = max(level, 1), rest[2:]
        elif rest in (b"..", b"."):
            level, rest = max(level, 1) + (rest == b".."), b""
        else:
            break
    if level > MAX_RELATIVE_LEVEL or (rest and _SPECIFIER.fullmatch(rest) is None):
        return None
    base = fragment.start_byte + len(raw) - len(rest)
    tokens: list[tuple[str, int, int]] = []
    for match in _IDENT.finditer(rest):
        try:
            text = match.group().decode("utf-8")
        except UnicodeDecodeError:
            return None
        tokens.append((text, base + match.start(), base + match.end()))
    return _Specifier(level, fragment.start_byte, fragment.end_byte, tuple(tokens))


def _named_import(walk: _Walk, spec: _Specifier, name: Node | None) -> None:
    simple = _identifier(walk, name) if name is not None and name.type == "identifier" else None
    if simple is None or name is None:
        return
    if spec.tokens:
        _reference(
            walk,
            simple,
            name.start_byte,
            name.end_byte,
            (".".join(token[0] for token in spec.tokens), spec.start, spec.end),
            spec.level,
        )
    elif spec.level:
        _reference(walk, simple, name.start_byte, name.end_byte, None, spec.level)


def _module_import(walk: _Walk, spec: _Specifier) -> None:
    """A whole-module import: the specifier itself is the target (see the module docstring)."""
    tokens = spec.tokens
    if len(tokens) == 1:
        text, start, end = tokens[0]
        _reference(walk, text, start, end, None, spec.level)
    elif len(tokens) == 2:
        (first, first_start, first_end), (text, start, end) = tokens
        _reference(walk, text, start, end, (first, first_start, first_end), spec.level)
    else:
        walk.capped += 1


def _enter_module_statement(walk: _Walk, node: Node) -> None:
    """``import``/``export ... from`` statements: one reference per name, or per module."""
    if node.has_error:
        return
    clause: Node | None = None
    source = node.child_by_field_name("source")
    if source is None and node.type == "import_statement":
        for child in node.children:
            if child.type == "import_require_clause":
                source = child.child_by_field_name("source")
                break
    if source is None:
        return
    spec = _specifier(walk, source)
    if spec is None:
        walk.capped += 1
        return
    whole = True
    for child in node.children:
        if child.type == "import_clause":
            clause = child
        elif child.type == "export_clause":
            whole = False
            for item in child.named_children:
                if item.type == "export_specifier":
                    _named_import(walk, spec, item.child_by_field_name("name"))
    if clause is not None:
        for child in clause.children:
            if child.type == "named_imports":
                for item in child.named_children:
                    if item.type == "import_specifier":
                        _named_import(walk, spec, item.child_by_field_name("name"))
        if any(child.type == "named_imports" for child in clause.children) and all(
            child.type not in ("identifier", "namespace_import") for child in clause.children
        ):
            whole = False
    if whole:
        _module_import(walk, spec)


def _enter(walk: _Walk, node: Node) -> bool:
    """Emit the node's tokens and open frames; True when its children must be walked."""
    kind = node.type
    walk.budget.node()
    if kind in _SKIPPED:
        return False
    if kind in _QUOTES and walk.ancestors and walk.ancestors[-1][0] == "string":
        return False
    if node.is_error or node.is_missing:
        walk.recovered += 1
        if walk.frames:
            walk.frames[-1].tainted = True
    depth = len(walk.frames)
    frame_open = walk.frames[-1] if walk.frames else None
    if frame_open is not None and frame_open.body_id == node.id:
        frame_open.header_end = len(walk.tokens)
    in_body = bool(walk.ancestors) and walk.ancestors[-1][0] == "class_body"
    decorated = walk.decorators_at
    if in_body:
        # In a class body ``@dec`` members are *siblings*: the next member owns them, so a
        # changed decorator is a changed member revision, not only a changed class.
        walk.decorators_at = (
            len(walk.tokens)
            if kind == "decorator" and decorated < 0
            else (decorated if kind == "decorator" else -1)
        )
    value_owner = walk.value_scopes.pop(node.id, None)
    if value_owner is not None:
        _push(walk, node, [value_owner])
    if kind in _FUNCTION_DECLARATIONS:
        _enter_named(walk, node, "function")
    elif kind in _CLASS_DECLARATIONS:
        _enter_named(walk, node, "class")
    elif kind == "interface_declaration":
        _enter_named(walk, node, "interface")
    elif kind in ("type_alias_declaration", "enum_declaration"):
        _enter_named(walk, node, "type")
    elif kind in _NAMESPACES:
        _enter_namespace(walk, node)
    elif kind in _MEMBERS:
        _enter_member(walk, node)
    elif kind in _VARIABLES:
        _enter_variables(walk, node)
    elif kind in ("call_expression", "new_expression"):
        _enter_call(walk, node)
    elif kind in ("import_statement", "export_statement"):
        _enter_module_statement(walk, node)
    if in_body and decorated >= 0 and kind != "decorator" and len(walk.frames) > depth:
        walk.frames[-1].tok_start = decorated
    if node.child_count == 0:
        walk.tokens.append(_leaf_token(walk, node))
        return False
    walk.tokens.append(frame(b"(" + kind.encode()))
    return True


def _leaf_token(walk: _Walk, node: Node) -> bytes:
    if not node.is_named:
        return frame(b"a:" + node.type.encode())
    text = walk.content[node.start_byte : node.end_byte]
    if node.type in _TEXT_LEAVES:
        # A newline in a literal is read as LF whatever the file uses: CRLF conversion is no edit.
        text = text.replace(b"\r\n", b"\n")
    return frame(b"n:" + node.type.encode() + b"=" + text)


def _finish_frame(walk: _Walk, entry: _Frame) -> None:
    if entry.tainted:
        # Release what the dropped symbols were charged so they cannot starve valid siblings.
        for sym in walk.syms[entry.first_sym :]:
            sym.alive = False
            walk.dropped += 1
            walk.budget.release(sym.qualified, sym.signature, walk.output)
        del walk.syms[entry.first_sym :]
        return
    stop = entry.header_end if entry.is_definition and entry.header_end >= 0 else len(walk.tokens)
    semantic = digest(_SEMANTIC_DOMAIN, walk.tokens[entry.tok_start :])
    signature = digest(_SIGNATURE_DOMAIN, walk.tokens[entry.tok_start : stop])
    for sym in entry.syms:
        sym.signature_digest = signature
        sym.semantic = semantic
    # Merkle step: an enclosing symbol sees this whole subtree as one token, so every token is
    # hashed once per level it belongs to a *new* subtree, not once per enclosing definition.
    del walk.tokens[entry.tok_start :]
    walk.tokens.append(frame(b"d:" + semantic.encode()))


def _exit(walk: _Walk, node: Node, opened: bool) -> None:
    if opened:
        walk.tokens.append(_CLOSE)
        walk.ancestors.pop()
    pushed = walk.scope_nodes.pop(node.id, 0)
    if pushed:
        del walk.scopes[-pushed:]
    if walk.frames and walk.frames[-1].node_id == node.id:
        _finish_frame(walk, walk.frames.pop())


def _traverse(tree: Tree, walk: _Walk) -> None:
    cursor = tree.walk()
    while True:
        node = cursor.node
        if node is None:
            return
        if _enter(walk, node) and cursor.goto_first_child():
            walk.ancestors.append((node.type, node.id, node.start_byte))
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
) -> list[_Sym] | None:
    """Same-file candidates by name (heuristic: lexical scope order, no import/alias logic).

    ``None`` when a scope binds more than ``_MAX_CANDIDATES`` of them: ambiguous noise that
    yields neither an edge nor a reference (and bounds the work per site).
    """
    if site.kind == "member":
        scope: _Sym | None = site.scope
        while scope is not None and scope.kind != "class":
            scope = scope.parent
        found = members.get((id(scope), site.name), []) if scope is not None else []
        if len(found) > _MAX_CANDIDATES:
            return None  # checked before filtering: O(1) however many same-named symbols
        found = [sym for sym in found if sym.kind == "method"]
        return found if len(found) <= _MAX_CANDIDATES else None
    scope = site.scope.parent if site.kind == "base" else site.scope
    first = True
    while scope is not None:
        # Names of an enclosing class body are not visible from its methods.
        if scope.kind != "class" or first:
            named = members.get((id(scope), site.name), [])
            if len(named) > _MAX_CANDIDATES:
                return None  # checked before filtering: O(1) however many share the name
            found = [s for s in named if s.kind in kinds]
            if found:
                return found
        first = False
        scope = scope.parent
    return []


@dataclass(eq=False)
class _Linked:
    relations: list[StructuralRelation] = field(default_factory=list)
    references: list[ParsedReference] = field(default_factory=list)


def _link(walk: _Walk, live: list[_Sym], index: dict[int, int]) -> _Linked:
    """Turn sites into same-file edges, or (when nothing in the file answers) references."""
    members = _index_members(live)
    linked = _Linked(references=list(walk.imports))
    per_relation: dict[int, int] = {}
    per_reference: dict[int | None, int] = {None: len(walk.imports)}
    seen: set[tuple[int, int, str]] = set()
    seen_references: set[tuple[str, str, int | None]] = set()
    memo: dict[tuple[int, str, str], list[_Sym] | None] = {}
    ordered = sorted(walk.sites, key=lambda site: site.kind != "base")  # stable: bases first
    for site in ordered:
        source = index.get(id(site.scope))
        if source is None and site.scope is not walk.root:
            continue  # the scope was dropped
        base = site.kind == "base"
        relation_kind: Literal["inherits", "calls"] = "inherits" if base else "calls"
        cache_key = (id(site.scope), site.kind, site.name)
        if "." in site.name:
            found: list[_Sym] | None = []
        else:
            if cache_key not in memo:
                memo[cache_key] = _resolve(members, site, _TYPE_KINDS if base else _CALLABLE_KINDS)
            found = memo[cache_key]
        if found is None:
            continue
        if found:
            if source is None:
                continue
            for target in found:
                key = (source, index[id(target)], relation_kind)
                if key in seen or per_relation.get(source, 0) >= MAX_RELATIONS_PER_SYMBOL:
                    continue
                if len(linked.relations) >= MAX_RELATIONS_PER_FILE:
                    break
                seen.add(key)
                per_relation[source] = per_relation.get(source, 0) + 1
                linked.relations.append(
                    StructuralRelation(
                        source_ref=str(source),
                        target_ref=str(key[1]),
                        kind=relation_kind,
                        start_byte=site.start,
                        end_byte=site.end,
                        evidence_kind=EVIDENCE_KIND,
                    )
                )
            continue
        if site.kind == "member":
            continue  # ``this.f()`` with no ``f`` here is inherited or dynamic: no name to carry
        reference_key = (site.kind, site.name, source)
        if reference_key in seen_references:
            continue
        seen_references.add(reference_key)
        limit = MAX_MODULE_REFERENCES if source is None else MAX_REFERENCES_PER_SYMBOL
        size = len(site.name.encode())
        if (
            per_reference.get(source, 0) >= limit
            or len(linked.references) >= MAX_REFERENCES_PER_FILE
            or walk.reference_bytes + size > _REFERENCE_NAME_LIMIT
        ):
            walk.capped += 1
            continue
        walk.reference_bytes += size
        per_reference[source] = per_reference.get(source, 0) + 1
        linked.references.append(
            ParsedReference(
                source=None if source is None else str(source),
                kind="inherit" if base else "call",
                target_name=site.name,
                start_byte=site.start,
                end_byte=site.end,
                evidence_kind=EVIDENCE_KIND,
                confidence="syntactic" if base else "heuristic",
            )
        )
    return linked


def _diagnostics(*counts: tuple[str, int]) -> tuple[ParsedDiagnostic, ...]:
    return tuple(
        ParsedDiagnostic(code=code, count=min(count, MAX_DIAGNOSTIC_COUNT))  # type: ignore[arg-type]
        for code, count in counts
        if count > 0
    )


def _degraded(source: SourceFile, reason: str) -> ParsedFile:
    """No structure, but never mistaken for an empty file: ``file_degraded`` plus its reason."""
    return ParsedFile(
        path=source.path,
        language=source.language,
        parser_fingerprint=FINGERPRINT,
        symbols=(),
        diagnostics=_diagnostics(("file_degraded", 1), (reason, 1)),
    )


def _parse_file(source: SourceFile, budget: Budget, output: list[int]) -> ParsedFile:
    mark = output[0]
    try:
        return _parse(source, budget, output)
    except WorkBudgetExceeded:
        output[0] = mark
        return _degraded(source, "work_budget_exceeded")


def _parse(source: SourceFile, budget: Budget, output: list[int]) -> ParsedFile:
    content = source.content()
    if not content:
        return ParsedFile(
            path=source.path, language=source.language, parser_fingerprint=FINGERPRINT, symbols=()
        )
    name = _module_name(source.path)
    root = _Sym(name.rsplit(".", 1)[-1], name, "module", 0, len(content), None)
    walk = _Walk(content, budget, output, root)
    if name and not walk.add(root, _Frame(-1, False, 0, 0)):
        return _degraded(source, "work_budget_exceeded")
    walk.scopes.append(root)
    _traverse(bounded_parse(_parser(_flavor(source.path)), content, budget), walk)
    if name:
        root.semantic = digest(_SEMANTIC_DOMAIN, walk.tokens)
        root.signature_digest = digest(_SIGNATURE_DOMAIN, ())
    live = [sym for sym in walk.syms if sym.alive]
    index = {id(sym): number for number, sym in enumerate(live)}
    linked = _link(walk, live, index)
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
        relations=tuple(linked.relations),
        references=tuple(linked.references),
        diagnostics=_diagnostics(
            ("syntax_recovered", walk.recovered),
            ("symbols_dropped", walk.dropped),
            ("references_capped", walk.capped),
        ),
    )


def parse_typescript(request: ParseRequest) -> ParsedModule:
    """Child-side entry: one ``ParsedFile`` per input file; the parent validates the result."""
    return safe_module(request, FINGERPRINT, _parse_file)


def typescript_adapter(
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
    sys.exit(serve(parse_typescript))
