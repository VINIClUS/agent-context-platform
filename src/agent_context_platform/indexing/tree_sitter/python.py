"""Syntax-only structural adapter for Python (tree-sitter), PLATFORM-033.

Run as ``python -m agent_context_platform.indexing.tree_sitter.python`` it is the untrusted
child of ``SandboxedAdapter`` (``ParseRequest`` on stdin, ``ParsedModule`` on stdout);
``python_adapter()`` builds that sandboxed adapter. It never opens a file: everything comes
on stdin and the grammar is loaded from site-packages only.

What is emitted
- ``module`` for the file (``__init__.py`` is the package), named from the path (Unicode
  identifier components are kept);
- ``class`` (nested too), ``function`` and ``method`` (sync and async), ``property`` for
  ``@property``/``@x.setter``/``@cached_property`` methods, module-level ``variable`` and
  ``constant`` (ALL_CAPS), class-level ``field`` and ``type`` for ``type X = ...``. An assignment
  binds in the module or class it lexically sits in even when nested in ``if``/``try``/``with``/
  loop blocks (``if TYPE_CHECKING: Alias = ...``); one inside a function body does not;
- ``inherits`` edges (a class to a same-file base class it names) and ``calls`` edges (a
  caller to a same-file function/method/class named at the call site);
- ``references`` (PLATFORM-032c), unresolved names the syntax mentions, for what a
  ``StructuralRelation`` cannot say because its target is not a symbol of this file:

  * ``import a.b`` and ``import a.b as c``: ``import``/``syntactic``, target ``a.b`` at the
    dotted name (the alias is not part of the range; it is ``alias="c"``, the name the import
    binds; ``from a import b as c`` likewise gives target ``b`` and ``alias="c"``);
  * ``from a.b import c, d``: one ``import`` per name, target ``c`` at the name, qualifier
    ``a.b`` at the module; ``from ..pkg import x``: qualifier ``pkg`` and ``relative_level`` 2;
    ``from . import x``: no qualifier, level 1;
  * star imports: ``from a.b import *`` references the MODULE (target ``a.b``, or ``pkg`` for
    ``from .pkg import *``) since ``*`` is not a name and the names it binds cannot be listed
    syntactically; ``from . import *`` names nothing and yields no reference;
  * calls to names this file does not define, ``print()``, ``Imported()`` and qualified
    ``mod.f()``/``a.b.f()`` (qualifier ``mod``/``a.b`` at the owner, owner a plain identifier
    or dotted name), as ``call``/``heuristic``. Local variables, parameters and comprehension
    names are not tracked, so ``callback()`` on a parameter is reported: that is what
    ``heuristic`` means. ``self.f()``/``cls.f()`` and calls on anything but a dotted name
    (``f().g()``, ``x[0].g()``) have no module to name and yield none;
  * bases defined elsewhere as ``inherit``/``syntactic`` (``class A(Base)``, ``class A(m.Base)``,
    ``class A(Base[T])`` names ``Base``; keyword and starred arguments are not bases).

  A call reference qualifier is the syntactic head, NOT a resolved module: locals, parameters and
  import aliases are not resolved here (``import a.b as c; c.f()`` gives qualifier ``c``
  and the import carries ``alias="c"``). A consumer
  must resolve a qualifier only when its first segment is bound by an import or a module-level
  name.

  A name bound in this file (a class, function or variable, also as the head of ``Acc.build()``)
  is NOT a reference: a same-file edge is a relation, and a name with more than 8 candidates in
  its scope yields nothing. The same edge is never both. References are deduplicated per
  (source, kind, level, qualifier, target), imports first, then bases, then calls, and capped
  (64 per symbol, 4096 at module level, 50k per file, name and output budgets); what does not
  fit is reported as ``references_capped``. Module-level references have ``source`` None.

Every relation is a *heuristic* structural candidate: ``evidence_kind`` is always
``tree_sitter`` and nothing here resolves scope, imports, aliases, ``obj.method()`` or
dynamic calls. Per source symbol the first call site of each target is kept (at most
``MAX_RELATIONS_PER_SYMBOL`` edges).

Diagnostics (closed codes with counts, no text): ``syntax_recovered`` counts the ERROR/MISSING
nodes tree-sitter recovered from; ``symbols_dropped`` the symbols dropped for them or for a
limit or an unrepresentable name; ``references_capped`` the references that did not fit;
``file_degraded`` plus ``work_budget_exceeded``/``symbols_dropped`` (see ``_common.safe_module``)
marks a file answered with no structure at all.

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
qualified through it, and so are the references made inside it. An error in a decorator
belongs to its own ``decorated_definition`` (which owns a marker frame): only that definition
goes, not the class around it. Precisely: an error inside a method's own tokens drops only that method
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
    bounded_parse,
    cut_signature,
    degraded_file,
    diagnostics,
    digest,
    dotted_name_at,
    frame,
    identifier_bytes,
    module_name,
    safe_module,
)
from agent_context_platform.indexing.tree_sitter.base import (
    EVIDENCE_KIND,
    MAX_MODULE_REFERENCES,
    MAX_NAME_BYTES,
    MAX_REFERENCES_PER_FILE,
    MAX_REFERENCES_PER_SYMBOL,
    MAX_RELATIONS_PER_FILE,
    MAX_RELATIONS_PER_SYMBOL,
    MAX_RELATIVE_LEVEL,
    MAX_SYMBOLS_PER_FILE,
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

LANGUAGE: Final = "python"
ADAPTER_NAME: Final = "agent-context-python-tree-sitter"
# Bump whenever the emitted structure or the fingerprint token stream changes.
ADAPTER_VERSION: Final = "3"
# Pinned in pyproject.toml/uv.lock; a test asserts these equal the installed distributions,
# so the child never reads package metadata at run time.
GRAMMAR_VERSIONS: Final = {"tree-sitter": "0.26.0", "tree-sitter-python": "0.25.0"}
FINGERPRINT: Final = parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS)

_SEMANTIC_DOMAIN: Final = b"agent-context/python/semantic/v1"
_SIGNATURE_DOMAIN: Final = b"agent-context/python/signature/v1"
_CLOSE: Final = frame(b")")
_MAX_CALL_SITES: Final = 200_000
_MAX_CANDIDATES: Final = 8
_MAX_BASES: Final = 64
_MAX_UNWRAP: Final = 16  # ``Base[T][U]...``: how many subscripts are peeled off a base
# An import statement with more children than this is ~4000 names: skipped and reported.
_MAX_IMPORT_CHILDREN: Final = 8_200
_MEMBER_OWNERS: Final = (b"self", b"cls")
_ANY_KIND: Final = frozenset(
    {
        "function",
        "method",
        "class",
        "interface",
        "type",
        "variable",
        "constant",
        "field",
        "property",
    }
)
_DEFINITIONS: Final = frozenset({"function_definition", "class_definition"})
_SKIPPED: Final = frozenset({"comment", "line_continuation", "string_end", ",", ";"})
_PROPERTY_NAMES: Final = frozenset({"property", "cached_property"})
_PROPERTY_ATTRIBUTES: Final = frozenset({"setter", "getter", "deleter", "cached_property"})
_CALLABLE_KINDS: Final = frozenset({"function", "method", "class"})
_CLASS_KINDS: Final = frozenset({"class"})

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
    # A marker frame (a ``decorated_definition``) only owns the taint of its decorators.
    merkle: bool = True
    first_site: int = 0
    first_import: int = 0


@dataclass(frozen=True, slots=True)
class _Site:
    scope: _Sym
    kind: str  # "name": ``f()``; "member": ``self.f()``; "base": a class base
    name: str
    start: int
    end: int
    # ``mod.f()``/``class A(m.B)``: the owner's dotted name and range (never ``self``/``cls``).
    qualifier: str | None = None
    qualifier_start: int = 0
    qualifier_end: int = 0


@dataclass(frozen=True, slots=True)
class _Candidate:
    """A reference before caps: ``owner`` is the scope symbol (the root scope is module level)."""

    owner: _Sym
    kind: str  # "import" | "call" | "inherit"
    target: str
    start: int
    end: int
    level: int = 0
    qualifier: str | None = None
    qualifier_start: int = 0
    qualifier_end: int = 0
    alias: tuple[str, int, int] | None = None  # the ``as`` name and its byte range


@dataclass(eq=False)
class _Walk:
    content: bytes
    budget: Budget
    output: list[int]
    root: _Sym
    syms: list[_Sym] = field(default_factory=list)
    sites: list[_Site] = field(default_factory=list)
    imports: list[_Candidate] = field(default_factory=list)
    errors: int = 0
    dropped: int = 0
    capped: int = 0
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
            self.dropped += 1
            return False
        key = (sym.qualified, sym.kind, sym.start, sym.end)
        if key in self.seen:  # ``a = a = 1``, ``a, a = v``: one construct, one symbol
            return False
        if not self.budget.take(sym.qualified, sym.signature, self.output):
            self.dropped += 1
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


def _open_frame(walk: _Walk, node_id: int, is_definition: bool, tok_start: int) -> _Frame:
    return _Frame(
        node_id, is_definition, tok_start, len(walk.syms), first_site=len(walk.sites),
        first_import=len(walk.imports),
    )  # fmt: skip


def _enter_definition(walk: _Walk, node: Node) -> None:
    parent_scope = walk.scopes[-1]
    decorated = bool(walk.ancestors) and walk.ancestors[-1][0] == "decorated_definition"
    if decorated:
        start, tok_start = walk.decorated[walk.ancestors[-1][1]]
    else:
        start, tok_start = node.start_byte, len(walk.tokens)
    entry = _open_frame(walk, node.id, True, tok_start)
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
        if simple is None:
            walk.dropped += 1  # not representable (too long, glued to non-ASCII, bad UTF-8)
        else:
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
        if bases is not None:
            _enter_bases(walk, sym, bases)


def _enter_bases(walk: _Walk, cls: _Sym, bases: Node) -> None:
    """Base sites of a class: ``B``, ``m.B``, ``B[T]`` (unwrapped to ``B``); no keywords/splats."""
    walk.capped += max(0, bases.named_child_count - _MAX_BASES)  # bases past the cap: reported
    for number in range(min(bases.named_child_count, _MAX_BASES)):
        base = bases.named_child(number)
        for _ in range(_MAX_UNWRAP):  # the subscript chain of ``Base[T][U]`` is bounded
            if base is None or base.type != "subscript":
                break
            base = base.child_by_field_name("value")
        else:
            if base is not None and base.type == "subscript":
                walk.capped += 1  # still wrapped after the last peel: the base is skipped
                continue
        site = None if base is None else _name_site(walk, cls, "base", base)
        if site is not None:
            walk.sites.append(site)


def _dotted(walk: _Walk, node: Node) -> str | None:
    """The dotted name a node spells, or None; a name over ``MAX_NAME_BYTES`` is counted."""
    if node.end_byte - node.start_byte > MAX_NAME_BYTES:
        walk.capped += 1  # representable syntax that the contract cannot carry: not silent
        return None
    return dotted_name_at(walk.content, node.start_byte, node.end_byte)


def _name_site(walk: _Walk, scope: _Sym, kind: str, node: Node) -> _Site | None:
    """A site for an identifier or a ``owner.name`` attribute whose owner is a dotted name."""
    if node.end_byte - node.start_byte > MAX_NAME_BYTES:  # a chain prefix can be the whole file
        walk.capped += 1  # too long to be a name: not silently ignored
        return None
    if node.type == "identifier":
        name = _dotted(walk, node)
        return None if name is None else _Site(scope, kind, name, *node.byte_range)
    if node.type != "attribute":
        return None
    owner = node.child_by_field_name("object")
    attribute = node.child_by_field_name("attribute")
    if owner is None or attribute is None or owner.type not in ("identifier", "attribute"):
        return None
    qualifier = _dotted(walk, owner)
    name = _dotted(walk, attribute)
    if qualifier is None or name is None or "." in name:
        return None
    if qualifier.split(".", 1)[0].encode() in _MEMBER_OWNERS:
        return None  # ``self.a.f()`` names no module
    return _Site(scope, kind, name, *attribute.byte_range, qualifier, *owner.byte_range)


def _assignment_targets(node: Node) -> list[Node]:
    """Flat identifier targets of ``a = b = 1``, ``x: int``, ``a, (b, *c) = ...``."""
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
        elif current.type in (
            "pattern_list",
            "tuple_pattern",
            "list_pattern",
            "list_splat_pattern",
        ):
            stack.extend(reversed(current.named_children))
        elif current.type == "identifier":
            found.append(current)
    found.sort(key=lambda item: item.start_byte)
    return found


def _enter_statement(walk: _Walk, node: Node) -> None:
    """Assignments/type aliases binding in the module or class the statement lexically sits in.

    ``walk.scopes[-1]`` is the innermost enclosing definition, so a statement nested in
    ``if``/``try``/``with``/loop/``match`` blocks still binds at module or class level, and one
    inside a function body (at any block depth) never does.
    """
    scope = walk.scopes[-1]
    if scope is None or not (scope is walk.root or scope.kind == "class"):
        return
    in_class = scope is not walk.root
    entry = _open_frame(walk, node.id, False, len(walk.tokens))
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
            walk.dropped += 1
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
    if scope is None or function is None:
        return
    if len(walk.sites) >= _MAX_CALL_SITES:
        walk.capped += 1
        return
    if function.type == "identifier":
        site = _name_site(walk, scope, "name", function)
    elif function.type == "attribute":
        owner = function.child_by_field_name("object")
        attribute = function.child_by_field_name("attribute")
        # ``owner`` may be the whole nested prefix of a chain: test its type and size first.
        if owner is None or attribute is None or owner.type not in ("identifier", "attribute"):
            return
        if (
            owner.type == "identifier"
            and owner.end_byte - owner.start_byte in (3, 4)
            and _text(walk.content, owner) in _MEMBER_OWNERS
        ):
            name = _dotted(walk, attribute)
            site = None if name is None else _Site(scope, "member", name, *attribute.byte_range)
        else:
            site = _name_site(walk, scope, "name", function)
    else:
        return
    if site is not None:
        walk.sites.append(site)


def _relative_module(header: Node) -> tuple[int, Node | None] | None:
    """(level, module name node) of a ``relative_import``: the dots, then an optional dotted name."""
    prefix = header.child(0)
    if prefix is None or prefix.type != "import_prefix" or prefix.child_count > 64:
        return None
    # Total dots over ALL children: ``.`` is 1, and a grammar that emits ``...`` as one token is 3.
    level = sum(len(dot.type) for dot in prefix.children if dot.type and set(dot.type) == {"."})
    return level, header.child(1) if header.child_count > 1 else None


def _alias(walk: _Walk, node: Node) -> tuple[str, int, int] | None:
    """The ``as`` name of an ``aliased_import`` with its range, or None."""
    if node.type != "aliased_import":
        return None
    alias = node.child_by_field_name("alias")
    if alias is None or alias.type != "identifier":
        return None
    name = _dotted(walk, alias)
    return None if name is None else (name, *alias.byte_range)


def _enter_import(walk: _Walk, node: Node) -> None:
    """``import a.b [as c]``, ``from [.]m import x [as y]``, ``from m import *`` (the module)."""
    scope = walk.scopes[-1]
    if scope is None or node.has_error:  # a recovered statement names nothing reliably
        return
    if node.child_count > _MAX_IMPORT_CHILDREN:
        walk.capped += node.child_count // 2
        return
    names: list[tuple[Node, str, tuple[str, int, int] | None]] = []
    for child in node.children_by_field_name("name"):
        item = child.child_by_field_name("name") if child.type == "aliased_import" else child
        if item is not None and item.type == "dotted_name":
            name = _dotted(walk, item)
            if name is not None:
                names.append((item, name, _alias(walk, child)))
    if node.type == "import_statement":
        for item, name, alias in names:
            walk.imports.append(_Candidate(scope, "import", name, *item.byte_range, alias=alias))
        return
    header = node.child_by_field_name("module_name")
    module, level = header, 0
    if header is not None and header.type == "relative_import":
        relative = _relative_module(header)
        if relative is None or relative[0] > MAX_RELATIVE_LEVEL:
            walk.capped += 1  # a level beyond the contract cannot be represented
            return
        level, module = relative
    qualifier: str | None = None
    if module is not None:
        qualifier = _dotted(walk, module) if module.type == "dotted_name" else None
        if qualifier is None:
            return
    if any(child.type == "wildcard_import" for child in node.children):
        # ``*`` is not a name: the reference is the module (none for ``from . import *``).
        if module is not None and qualifier is not None:
            walk.imports.append(_Candidate(scope, "import", qualifier, *module.byte_range, level))
        return
    for item, name, alias in names:
        if module is None:
            walk.imports.append(
                _Candidate(scope, "import", name, *item.byte_range, level, alias=alias)
            )
        else:
            walk.imports.append(
                _Candidate(
                    scope,
                    "import",
                    name,
                    *item.byte_range,
                    level,
                    qualifier,
                    *module.byte_range,
                    alias=alias,
                )
            )


def _enter(walk: _Walk, node: Node) -> bool:
    """Emit the node's tokens and open frames; True when its children must be walked."""
    kind = node.type
    walk.budget.node()
    if node.is_error or node.is_missing:
        walk.errors += 1
        if walk.frames:
            walk.frames[-1].tainted = True
    if kind in _SKIPPED:
        return False
    if node.is_named and walk.ancestors and walk.ancestors[-1][0] in ("module", "block"):
        walk.first_child.setdefault(walk.ancestors[-1][1], node.id)
    frame_open = walk.frames[-1] if walk.frames else None
    if frame_open is not None and frame_open.body_id == node.id:
        frame_open.header_end = len(walk.tokens)
    if kind == "expression_statement" and _is_docstring(walk, node):
        return False
    if kind == "decorated_definition":
        walk.decorated[node.id] = (node.start_byte, len(walk.tokens))
        walk.property_decorator = False
        # Owns the taint of its decorators: an error there drops this definition only.
        marker = _open_frame(walk, node.id, False, len(walk.tokens))
        marker.merkle = False
        walk.frames.append(marker)
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
    elif kind in ("import_statement", "import_from_statement"):
        _enter_import(walk, node)
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
        walk.dropped += len(walk.syms) - entry.first_sym
        del walk.syms[entry.first_sym :]
        # Whatever was called or imported inside a dropped frame (definition, decorated marker or
        # assignment) belongs to symbols that no longer exist: roll it back with them.
        del walk.sites[entry.first_site :]
        del walk.imports[entry.first_import :]
        return
    if not entry.merkle:
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


class _Index:
    """Same-file symbols by (parent, name), filtered by kind once and then looked up in O(1)."""

    def __init__(self, syms: Sequence[_Sym]) -> None:
        self.members: dict[tuple[int, str], list[_Sym]] = {}
        self.names: set[str] = set()
        for sym in syms:
            if sym.parent is not None:
                self.members.setdefault((id(sym.parent), sym.simple), []).append(sym)
                self.names.add(sym.simple)
        self._filtered: dict[tuple[int, str, frozenset[str]], list[_Sym]] = {}

    def find(self, scope: _Sym, name: str, kinds: frozenset[str]) -> list[_Sym]:
        key = (id(scope), name, kinds)
        found = self._filtered.get(key)
        if found is None:
            found = [s for s in self.members.get((id(scope), name), []) if s.kind in kinds]
            self._filtered[key] = found
        return found


def _resolve(
    index: _Index, scope_of: _Sym, kind: str, name: str, kinds: frozenset[str]
) -> list[_Sym] | None:
    """Same-file candidates by name (lexical scope order, no import/alias logic).

    ``[]``: nothing in this file binds the name. ``None``: more than ``_MAX_CANDIDATES``
    definitions in the scope that binds it, ambiguous noise that yields no edge and no reference.
    """
    if name not in index.names:
        return []
    if kind == "member":
        scope: _Sym | None = scope_of
        while scope is not None and scope.kind != "class":
            scope = scope.parent
        if scope is None:
            return []
        found = [s for s in index.members.get((id(scope), name), []) if s.kind == "method"]
        return found if len(found) <= _MAX_CANDIDATES else None
    scope = scope_of.parent if kind == "base" else scope_of
    first = True
    while scope is not None:
        # Names of an enclosing class body are not visible from its methods.
        if scope.kind != "class" or first:
            found = index.find(scope, name, kinds)
            if len(found) > _MAX_CANDIDATES:
                return None
            if found:
                return found
        first = False
        scope = scope.parent
    return []


def _link(
    walk: _Walk, syms: list[_Sym], numbers: dict[int, int]
) -> tuple[tuple[StructuralRelation, ...], list[_Candidate]]:
    """Resolve every site: a same-file target is a relation, an unbound name a reference."""
    index = _Index(syms)
    per_source: dict[int, int] = {}
    seen: set[tuple[int, int, str]] = set()
    memo: dict[tuple[int, str, str, bool], list[_Sym] | None] = {}
    out: list[StructuralRelation] = []
    references: list[_Candidate] = []
    ordered = sorted(walk.sites, key=lambda site: site.kind != "base")  # stable: bases first
    for site in ordered:
        source = numbers.get(id(site.scope))
        if source is None and site.scope is not walk.root:
            continue  # the scope was dropped: so is everything inside it
        base = site.kind == "base"
        qualified = site.qualifier is not None
        if qualified:
            # ``Acc.build()``: the head names something of this file, so it is not non-local.
            name, kinds = (site.qualifier or "").split(".", 1)[0], _ANY_KIND
        else:
            name, kinds = site.name, _CLASS_KINDS if base else _CALLABLE_KINDS
        cache_key = (id(site.scope), site.kind, name, qualified)
        if cache_key not in memo:
            memo[cache_key] = _resolve(
                index, site.scope, "name" if qualified else site.kind, name, kinds
            )
        found = memo[cache_key]
        if found is None:
            walk.capped += 1  # more than _MAX_CANDIDATES in scope: no edge, no reference, counted
            continue
        if not found and site.kind != "member" and not qualified:
            # Nothing callable/class binds the name; a VARIABLE (``f = imported()``) may. Its
            # target is not knowable syntactically, so it is neither linked nor reported as an
            # external reference: bound in this file by any kind means no unresolved reference.
            bound_key = (id(site.scope), "bound", name, False)
            if bound_key not in memo:
                memo[bound_key] = _resolve(index, site.scope, site.kind, name, _ANY_KIND)
            if memo[bound_key]:
                continue
        if not found:
            if site.kind != "member":
                references.append(
                    _Candidate(
                        site.scope, "inherit" if base else "call", site.name, site.start, site.end,
                        0, site.qualifier, site.qualifier_start, site.qualifier_end,
                    )
                )  # fmt: skip
            continue
        if qualified or source is None:
            continue  # bound in this file, never a reference; a relation needs an emitted source
        for target in found:
            key = (source, numbers[id(target)], "inherits" if base else "calls")
            if key in seen:
                continue
            if (
                per_source.get(source, 0) >= MAX_RELATIONS_PER_SYMBOL
                or len(out) >= MAX_RELATIONS_PER_FILE
            ):
                # The contract has no relations code: a dropped relation is reported as
                # ``references_capped`` (the closest one). Later sites still become references.
                walk.capped += 1
                continue
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
    return tuple(out), references


def _references(
    walk: _Walk, candidates: list[_Candidate], numbers: dict[int, int]
) -> tuple[ParsedReference, ...]:
    """Cap and deduplicate: first site per (source, kind, level, qualifier, target) wins.

    Candidates arrive imports first, then bases, then calls, so a busy scope drops calls (never
    its imports) when it hits a cap. Everything dropped is counted for ``references_capped``.
    """
    out: list[ParsedReference] = []
    per_source: dict[int | None, int] = {}
    seen: set[tuple[int | None, str, int, str | None, str, str | None]] = set()
    for item in candidates:
        source: int | None = None
        if item.owner is not walk.root:
            source = numbers.get(id(item.owner))
            if source is None:
                continue
        key = (
            source,
            item.kind,
            item.level,
            item.qualifier,
            item.target,
            None if item.alias is None else item.alias[0],
        )
        if key in seen:
            continue
        limit = MAX_MODULE_REFERENCES if source is None else MAX_REFERENCES_PER_SYMBOL
        names = (
            len(item.target.encode())
            + len((item.qualifier or "").encode())
            + len("" if item.alias is None else item.alias[0].encode())
        )
        if (
            per_source.get(source, 0) >= limit
            or len(out) >= MAX_REFERENCES_PER_FILE
            or not walk.budget.take_reference(names, walk.output)
        ):
            walk.capped += 1
            continue
        seen.add(key)
        per_source[source] = per_source.get(source, 0) + 1
        out.append(
            ParsedReference(
                source=None if source is None else str(source),
                kind=item.kind,  # type: ignore[arg-type]
                target_name=item.target,
                relative_level=item.level,
                start_byte=item.start,
                end_byte=item.end,
                qualifier=item.qualifier,
                qualifier_start_byte=None if item.qualifier is None else item.qualifier_start,
                qualifier_end_byte=None if item.qualifier is None else item.qualifier_end,
                alias=None if item.alias is None else item.alias[0],
                alias_start_byte=None if item.alias is None else item.alias[1],
                alias_end_byte=None if item.alias is None else item.alias[2],
                evidence_kind=EVIDENCE_KIND,
                confidence="heuristic" if item.kind == "call" else "syntactic",
            )
        )
    return tuple(out)


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
        return degraded_file(source, FINGERPRINT, "symbols_dropped")
    walk.scopes.append(root)
    _traverse(bounded_parse(_PARSER, content, budget), walk)
    if name:
        root.semantic = digest(_SEMANTIC_DOMAIN, walk.tokens)
        root.signature_digest = digest(_SIGNATURE_DOMAIN, ())
    live = [sym for sym in walk.syms if sym.alive]
    numbers = {id(sym): number for number, sym in enumerate(live)}
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
    relations, candidates = _link(walk, live, numbers)
    references = _references(walk, [*walk.imports, *candidates], numbers)
    return ParsedFile(
        path=source.path,
        language=source.language,
        parser_fingerprint=FINGERPRINT,
        symbols=symbols,
        relations=relations,
        references=references,
        diagnostics=diagnostics(
            syntax_recovered=walk.errors,
            symbols_dropped=walk.dropped,
            references_capped=walk.capped,
        ),
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
