"""Syntax-only structural adapter for Go (tree-sitter), PLATFORM-035.

Run as ``python -m agent_context_platform.indexing.tree_sitter.go`` it is the untrusted child
of ``SandboxedAdapter`` (``ParseRequest`` on stdin, ``ParsedModule`` on stdout);
``go_adapter()`` builds that sandboxed adapter. It never opens a file: everything comes on
stdin and the grammar is loaded from site-packages only.

Naming (decision, PLATFORM-035)
Go's unit is the package, i.e. the directory, not the file. A file's ``module`` symbol is named
from the path plus the package clause: the trailing run of identifier components of the file's
*directory* followed by the package name, the name being left out when it equals the last
directory component (``internal/store/db.go`` with ``package store`` is ``internal.store``;
``cmd/tool/main.go`` with ``package main`` is ``cmd.tool.main``; a root ``main.go`` is ``main``).
Every other symbol of the file is qualified through it: ``internal.store.Open``,
``internal.store.DB.Close``. All files of one package therefore share the prefix, so a method
declared in another file than its type still lands under the type. Directory components that
are not identifiers (``my-pkg``, ``2024``) end the run. The module symbol spans the whole file
(one per file, like every adapter), and a file without a usable package clause is named from
the directory alone.

What is emitted
- ``module``; ``class`` (struct types), ``interface`` and ``type`` (aliases and every other
  defined type); ``function``; ``method`` (value, pointer and generic receivers, qualified
  ``T.m`` through the receiver type: rule (b) when the type is emitted, rule (c) otherwise, and
  interface methods under their interface); package-level ``constant`` and ``variable``.
  Function-local declarations and struct fields are not symbols.
- ``import`` references (single, grouped, aliased, dot, blank, raw or interpreted string).
  The import path is a string literal, so the whole path is the ``qualifier`` (its range is the
  literal's content, which is what lets ``/`` separate; ``example.com/a/b`` is ``example.com.a.b``)
  and ``target_name`` is the last path token that is not a ``vN`` suffix, at its own range
  (``yaml`` for ``gopkg.in/yaml.v3``). An explicit local name is ``alias`` at its own token
  (``import m "example.com/x/mod"``: target ``mod``, ``alias="m"``); ``import _ "x"`` reports
  ``alias="_"`` (the blank identifier, which binds nothing usable). A dot import has no
  identifier to report, so it is indistinguishable from a plain import. A path that cannot be
  expressed as a qualifier (``go-yaml``, a leading digit, an escape sequence) has no qualifier;
  an aliased import whose final path element cannot be decoded (an escape) is still reported,
  with the alias, against the last identifier token no escape touches.
- ``call`` references: ``pkg.F()`` where ``pkg`` names an import of the file (its alias, or the
  path's last element, without a ``vN`` suffix or a ``go-`` style prefix) is qualified by the
  package identifier; ``F()`` that no function of this file matches is unqualified.
  ``x.M()`` on anything else is not a reference: syntax cannot tell a variable from a package.
- ``inherit`` references for embedded struct/interface types (``pkg.T`` qualified by the package);
  same-file embedded types and same-file callees become ``inherits``/``calls`` relations, at most
  ``MAX_RELATIONS_PER_SYMBOL`` per source, and none for a name with more than 8 candidates.
  ``r.m()`` on the method's own receiver name is a relation to the receiver type's methods.

Every call is *heuristic* (``confidence="heuristic"``, never SCIP-semantic): nothing resolves
scope, shadowing, imports or interfaces.

Syntax errors. tree-sitter recovers; a declaration (or a spec of a grouped declaration) that
contains an ERROR or MISSING node is not emitted, with everything nested in it, and reported as
``syntax_recovered`` (a flag: its count is 1 whenever the file has syntax errors). A symbol the limits refuse is counted as ``symbols_dropped``, references over
their caps as ``references_capped``; a file over the work budget degrades alone (``file_degraded``
with ``work_budget_exceeded``, no structure) through ``_common.safe_module``.

Fingerprints hash the normalized token stream of the declaration: comments, commas, semicolons
and whitespace are not part of it, and the CR that Go discards from raw strings is dropped, so
gofmt and CRLF conversion do not create a revision. The signature digest covers the header (up to
the body of a function, up to the value of a constant/variable, the whole type declaration).

Robustness: the walk is an iterative cursor loop, source text comes from slicing the input bytes
(never ``Node.text``, never ``Node.parent``), the parse runs through ``_common.bounded_parse`` and
the per-file node budget plus the per-request CPU backstop of ``_common.Budget`` apply.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Final

import tree_sitter_go
from tree_sitter import Language, Node, Parser, Tree

from agent_context_platform.indexing.tree_sitter._common import (
    IDENT,
    Budget,
    bounded_parse,
    cut_signature,
    degraded_file,
    digest,
    frame,
    identifier_bytes,
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

LANGUAGE: Final = "go"
ADAPTER_NAME: Final = "agent-context-go-tree-sitter"
# Bump whenever the emitted structure or the fingerprint token stream changes.
ADAPTER_VERSION: Final = "1"
# Pinned in pyproject.toml/uv.lock; a test asserts these equal the installed distributions,
# so the child never reads package metadata at run time.
GRAMMAR_VERSIONS: Final = {"tree-sitter": "0.26.0", "tree-sitter-go": "0.25.0"}
FINGERPRINT: Final = parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS)

_SEMANTIC_DOMAIN: Final = b"agent-context/go/semantic/v1"
_SIGNATURE_DOMAIN: Final = b"agent-context/go/signature/v1"
_MAX_SITES: Final = 200_000
_MAX_CANDIDATES: Final = 8
_MAX_HEADER: Final = 4096
_MAX_IMPORT_PATH: Final = 1024
# Names of references share the file-wide name budget with the symbols (which get the other half).
_SKIPPED: Final = frozenset({"comment", ",", ";"})
_SPEC_HOLDERS: Final = frozenset({"type_declaration", "const_declaration", "var_declaration"})
_TYPE_SHAPES: Final = frozenset({"type_identifier", "qualified_type", "generic_type"})
_WRAPPERS: Final = frozenset({"pointer_type", "parenthesized_type"})
_PREDECLARED: Final = frozenset(
    [
        "append",
        "cap",
        "clear",
        "close",
        "complex",
        "copy",
        "delete",
        "imag",
        "len",
        "make",
        "max",
        "min",
        "new",
        "panic",
        "print",
        "println",
        "real",
        "recover",
        "bool",
        "byte",
        "complex64",
        "complex128",
        "error",
        "float32",
        "float64",
        "int",
        "int8",
        "int16",
        "int32",
        "int64",
        "rune",
        "string",
        "uint",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "uintptr",
        "any",
        "comparable",
    ]
)
_VERSION: Final = re.compile(rb"v[0-9]+")
_NAME_PARTS: Final = re.compile(rb"[.\-]")
_ASCII_SPACE: Final = b" \t\r\n\f\v"
_BOM: Final = b"\xef\xbb\xbf"
_CLOSE: Final = frame(b")")

_PARSER: Final = Parser(Language(tree_sitter_go.language()))


@dataclass(eq=False, slots=True)
class _Sym:
    simple: str
    qualified: str
    kind: SymbolKind
    start: int
    end: int
    signature: str = ""
    signature_digest: str = ""
    semantic: str = ""
    recv_name: str = ""
    recv_type: str = ""


@dataclass(eq=False, slots=True)
class _Open:
    """An open declaration: where its tokens start and which symbols it owns."""

    node_id: int
    tok_start: int
    syms: list[_Sym]
    body_id: int = -1
    header_end: int = -1
    # Multi-name specs (``var a, b = x, y``): token position of every name and value element,
    # so each symbol is digested from its own declarator rather than from the whole spec.
    marks: dict[int, int] = field(default_factory=dict)
    names: list[int] = field(default_factory=list)  # node ids of every name, in order
    values: list[int] = field(default_factory=list)
    owners: list[tuple[_Sym, int]] = field(default_factory=list)  # symbol, index into names
    # Const specs: node id of the last name (the type and value tokens follow it), and whether
    # the spec has neither, so that it repeats the previous spec's type and expression (iota).
    const_mark: int = -1
    implicit: bool = False


@dataclass(frozen=True, slots=True)
class _Name:
    text: str
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class _Site:
    scope: _Sym | None  # the enclosing function/method; None at package level
    kind: str  # "call", "qcall", "base", "qbase"
    name: _Name
    qualifier: _Name | None = None


@dataclass(frozen=True, slots=True)
class _ImportRef:
    name: _Name
    qualifier: _Name | None
    alias: _Name | None = None


@dataclass(eq=False)
class _Walk:
    content: bytes
    budget: Budget
    output: list[int]
    root: _Sym
    module_q: str
    syms: list[_Sym] = field(default_factory=list)
    sites: list[_Site] = field(default_factory=list)
    imports: list[_ImportRef] = field(default_factory=list)
    import_names: set[str] = field(default_factory=set)
    tokens: list[bytes] = field(default_factory=list)
    opens: list[_Open] = field(default_factory=list)
    ancestors: list[str] = field(default_factory=list)
    seen: set[tuple[str, str, int, int]] = field(default_factory=set)
    scope: _Sym | None = None
    scope_node: int = -1
    skip_node: int = -1  # inside a dropped declaration: its calls belong to nobody
    type_sym: _Sym | None = None
    tainted: int = 0
    dropped: int = 0
    const_tail: tuple[list[bytes], list[bytes]] = field(default_factory=lambda: ([], []))
    oversized: int = 0  # import names over the byte limit: reported as references_capped

    def add(self, sym: _Sym) -> bool:
        """Register a symbol if limits allow; a duplicate construct is silently one symbol."""
        key = (sym.qualified, sym.kind, sym.start, sym.end)
        if key in self.seen:
            return False
        if len(self.syms) >= MAX_SYMBOLS_PER_FILE or not self.budget.take(
            sym.qualified, sym.signature, self.output
        ):
            self.dropped += 1
            return False
        self.seen.add(key)
        self.syms.append(sym)
        return True


def _name(content: bytes, node: Node) -> _Name | None:
    """A whole-token identifier of a node that is name-sized, sliced from the source."""
    if node.end_byte - node.start_byte > MAX_NAME_BYTES:
        return None
    text = identifier_bytes(
        content[node.start_byte : node.end_byte], content, node.start_byte, node.end_byte
    )
    return None if text is None else _Name(text, node.start_byte, node.end_byte)


def _declared(walk: _Walk, node: Node | None) -> _Name | None:
    """The declared name; one that cannot be emitted (too long, not a whole token) is counted."""
    if node is None:
        return None
    found = _name(walk.content, node)
    if found is None:
        walk.dropped += 1
    return found


def _join(*parts: str) -> str:
    return ".".join(part for part in parts if part)


def _header(content: bytes, start: int, stop: int) -> str:
    return cut_signature(content[start : min(stop, start + _MAX_HEADER)])


def _directory_name(path: str) -> list[str]:
    run: list[str] = []
    for part in reversed(path.split("/")[:-1]):
        if not part.isidentifier() or len(part.encode()) > MAX_NAME_BYTES:
            break
        run.append(part)
    return run[::-1]


def _package_name(root: Node, content: bytes) -> tuple[_Name | None, Node | None]:
    """The package clause follows the leading comments (Go allows nothing else before it)."""
    cursor = root.walk()
    try:
        if not cursor.goto_first_child():
            return None, None
        while True:
            child = cursor.node
            if child is None:
                return None, None
            if child.type == "package_clause":
                name = child.named_child(0)
                if child.has_error or name is None or name.type != "package_identifier":
                    return None, None
                # A name the limits reject is not an absent clause: the caller must not guess.
                return _name(content, name), child
            if child.type != "comment" or not cursor.goto_next_sibling():
                return None, None
    finally:
        del cursor


def _module_qualified(path: str, package: _Name | None) -> str:
    parts = _directory_name(path)
    if package is not None and (not parts or parts[-1] != package.text):
        parts.append(package.text)
    return ".".join(parts)


def _leaf_token(content: bytes, node: Node) -> bytes:
    if node.is_named:
        text = content[node.start_byte : node.end_byte]
        if node.type == "raw_string_literal_content":
            text = text.replace(b"\r", b"")  # Go discards CR in raw strings: CRLF is no edit
        return frame(b"n:" + node.type.encode() + b"=" + text)
    return frame(b"a:" + node.type.encode())


def _unwrap(node: Node | None) -> Node | None:
    """``*T``, ``(T)`` and ``T[int]`` to the named type they are made of."""
    for _ in range(16):
        if node is None:
            return None
        if node.type in _WRAPPERS:
            node = node.named_child(0)
        elif node.type == "generic_type":
            node = node.child_by_field_name("type")
        else:
            return node
    return None


def _type_name(content: bytes, node: Node | None) -> tuple[_Name | None, _Name | None]:
    """(package qualifier, type name) of an embedded/receiver type; (None, None) if not a name."""
    named = _unwrap(node)
    if named is None:
        return None, None
    if named.type == "type_identifier":
        return None, _name(content, named)
    if named.type == "qualified_type":
        package = named.child_by_field_name("package")
        name = named.child_by_field_name("name")
        if package is not None and name is not None:
            return _name(content, package), _name(content, name)
    return None, None


def _open(walk: _Walk, node: Node, syms: list[_Sym], body: Node | None = None) -> None:
    walk.opens.append(_Open(node.id, len(walk.tokens), syms, body.id if body is not None else -1))


def _enter_function(walk: _Walk, node: Node) -> None:
    body = node.child_by_field_name("body")
    name_node = node.child_by_field_name("name")
    method = node.type == "method_declaration"
    simple = _declared(walk, name_node)
    sym: _Sym | None = None
    if not node.has_error and simple is not None and simple.text != "_":
        receiver = ""
        recv_name = ""
        ok = True
        if method:
            receiver_list = node.child_by_field_name("receiver")
            param = receiver_list.named_child(0) if receiver_list is not None else None
            _, type_name = (
                _type_name(walk.content, param.child_by_field_name("type"))
                if param is not None
                and receiver_list is not None
                and receiver_list.named_child_count == 1
                else (None, None)
            )
            ok = type_name is not None
            if type_name is not None and param is not None:
                receiver = type_name.text
                given = param.child_by_field_name("name")
                found = _name(walk.content, given) if given is not None else None
                recv_name = found.text if found is not None else ""
        if ok:
            stop = body.start_byte if body is not None else node.end_byte
            candidate = _Sym(
                simple.text,
                _join(walk.module_q, receiver, simple.text),
                "method" if method else "function",
                node.start_byte,
                node.end_byte,
                _header(walk.content, node.start_byte, stop),
                recv_name=recv_name,
                recv_type=receiver,
            )
            if walk.add(candidate):
                sym = candidate
    elif node.has_error:
        walk.tainted += 1
    if sym is None:
        walk.skip_node = node.id
        walk.scope = None
        return
    walk.scope, walk.scope_node = sym, node.id
    _open(walk, node, [sym], body)


def _enter_type(walk: _Walk, node: Node) -> None:
    name_node = node.child_by_field_name("name")
    shape = node.child_by_field_name("type")
    simple = _declared(walk, name_node)
    walk.type_sym = None
    if node.has_error:
        walk.tainted += 1
    elif simple is not None and simple.text != "_" and shape is not None:
        kind: SymbolKind = "type"
        if node.type == "type_spec" and shape.type == "struct_type":
            kind = "class"
        elif node.type == "type_spec" and shape.type == "interface_type":
            kind = "interface"
        candidate = _Sym(
            simple.text,
            _join(walk.module_q, simple.text),
            kind,
            node.start_byte,
            node.end_byte,
            _header(walk.content, node.start_byte, node.end_byte),
        )
        if walk.add(candidate):
            walk.type_sym = candidate
            _open(walk, node, [candidate])
            return
    walk.skip_node = node.id


def _enter_value_spec(walk: _Walk, node: Node) -> None:
    value = node.child_by_field_name("value")
    kind: SymbolKind = "constant" if node.type == "const_spec" else "variable"
    if node.has_error:
        walk.tainted += 1
        walk.skip_node = node.id
        return
    stop = value.start_byte if value is not None else node.end_byte
    signature = _header(walk.content, node.start_byte, stop)
    made: list[_Sym] = []
    owners: list[tuple[_Sym, int]] = []
    name_ids: list[int] = []
    for child in node.children_by_field_name("name"):
        if child.type != "identifier":  # the field also lists the commas between names
            continue
        name_ids.append(child.id)
        simple = _declared(walk, child)
        if simple is None or simple.text == "_":
            continue
        candidate = _Sym(
            simple.text,
            _join(walk.module_q, simple.text),
            kind,
            node.start_byte,
            node.end_byte,
            signature,
        )
        if walk.add(candidate):
            made.append(candidate)
            owners.append((candidate, len(name_ids) - 1))
    _open(walk, node, made, value)
    entry = walk.opens[-1]
    if kind == "constant" and name_ids:
        entry.const_mark = name_ids[-1]
        entry.marks[name_ids[-1]] = -1
        entry.implicit = value is None and node.child_by_field_name("type") is None
    if len(name_ids) > 1 and made:
        entry.names, entry.owners = name_ids, owners
        if value is not None:
            entry.values = [item.id for item in value.named_children if item.type != "comment"]
        entry.marks.update(dict.fromkeys(entry.names + entry.values, -1))


def _enter_interface_member(walk: _Walk, node: Node) -> None:
    owner = walk.type_sym
    if owner is None:
        return
    if node.type == "type_elem":
        child = node.named_child(0) if node.named_child_count == 1 else None
        if child is not None and child.type in _TYPE_SHAPES:
            _embedded(walk, owner, child)
        return
    name_node = node.child_by_field_name("name")
    simple = _declared(walk, name_node)
    if node.has_error:
        walk.tainted += 1
        walk.skip_node = node.id
    elif simple is not None and simple.text != "_":
        candidate = _Sym(
            simple.text,
            _join(walk.module_q, owner.simple, simple.text),
            "method",
            node.start_byte,
            node.end_byte,
            _header(walk.content, node.start_byte, node.end_byte),
        )
        if walk.add(candidate):
            _open(walk, node, [candidate])


def _embedded(walk: _Walk, owner: _Sym, node: Node) -> None:
    package, name = _type_name(walk.content, node)
    if name is None or len(walk.sites) >= _MAX_SITES:
        return
    walk.sites.append(_Site(owner, "base" if package is None else "qbase", name, package))


def _enter_field(walk: _Walk, node: Node) -> None:
    owner = walk.type_sym
    if owner is None or node.child_by_field_name("name") is not None:
        return  # a named field, or the struct was dropped
    field_type = node.child_by_field_name("type")
    if field_type is not None:
        _embedded(walk, owner, field_type)


def _enter_import(walk: _Walk, node: Node) -> None:
    name_node = node.child_by_field_name("name")
    path = node.child_by_field_name("path")
    if node.has_error or path is None or path.end_byte - path.start_byte > _MAX_IMPORT_PATH:
        return
    content = walk.content
    start, end = path.start_byte + 1, path.end_byte - 1
    quote = content[path.start_byte : start]
    if quote not in (b'"', b"`") or content[end : end + 1] != quote or end <= start:
        return
    body = content[start:end]
    if b"\n" in body or quote in body:
        return
    # An escape (interpreted string only) makes the raw bytes unreliable: keep just the final
    # path element when no escape touches it, and give it no qualifier.
    escaped = b"\\" in body
    base = 0
    alias: _Name | None = None
    if name_node is not None and name_node.type in ("package_identifier", "blank_identifier"):
        alias = _name(content, name_node)
        if alias is None:
            walk.oversized += 1
            return
    scanned = body
    tokens: list[re.Match[bytes]] = []
    if escaped:
        base = body.rfind(b"/") + 1
        if base > 0 and b"\\" not in body[base:]:
            scanned = body[base:]
        elif alias is None:
            return
        else:
            # The alias needs no path decoding: keep the import, targeting the last identifier
            # token that no escape sequence touches (the path's final element cannot be read).
            base = 0
            for item in reversed(list(IDENT.finditer(body))):
                near = body[max(item.start() - 1, 0) : item.end() + 1]
                if b"\\" not in near:
                    tokens = [item]
                    break
            if not tokens:
                return
    if not tokens:
        tokens = list(IDENT.finditer(scanned))
    if not tokens:
        return
    try:
        names = [item.group().decode() for item in tokens]
    except UnicodeDecodeError:
        return
    if any(len(item.group()) > MAX_NAME_BYTES for item in tokens):  # UTF-8 bytes, as the contract
        walk.oversized += 1
        return
    # The target is the last path token that is not a "vN" major-version suffix; the local name an
    # import binds (its alias, or ``_``) is reported separately as ``alias``.
    pick = len(tokens) - 1
    while pick > 0 and _VERSION.fullmatch(tokens[pick].group()):
        pick -= 1
    offset = start + base
    target = _Name(names[pick], offset + tokens[pick].start(), offset + tokens[pick].end())
    last = tokens[-1]
    # Clean: the path is nothing but identifiers joined by "." or "/", so the qualifier is the path.
    clean = (
        not escaped
        and tokens[0].start() == 0
        and last.end() == len(body)
        and all(
            body[before.end() : after.start()].strip(_ASCII_SPACE) in (b".", b"/")
            for before, after in pairwise(tokens)
        )
    )
    joined = ".".join(names)
    fits = len(joined.encode()) <= MAX_NAME_BYTES
    if clean and not fits:
        walk.oversized += 1
    qualifier = _Name(joined, start, end) if clean and fits else None
    walk.imports.append(_ImportRef(target, qualifier, alias))
    if name_node is not None and name_node.type == "package_identifier":
        if alias is not None:
            walk.import_names.add(alias.text)
    elif name_node is None:
        walk.import_names.update(_default_names(scanned))


def _default_names(path: bytes) -> set[str]:
    """Package names an unaliased import may bind: the last element, minus ``vN``/``go-``."""
    elements = path.split(b"/")
    if len(elements) > 1 and _VERSION.fullmatch(elements[-1]):
        elements.pop()
    found: set[str] = set()
    for part in _NAME_PARTS.split(elements[-1]):
        if IDENT.fullmatch(part) and not _VERSION.fullmatch(part):
            try:
                found.add(part.decode())
            except UnicodeDecodeError:
                continue
    return found


def _enter_call(walk: _Walk, node: Node) -> None:
    if walk.skip_node != -1 or len(walk.sites) >= _MAX_SITES:
        return
    function = node.child_by_field_name("function")
    while function is not None and function.type in (
        "index_expression",
        "parenthesized_expression",
    ):
        # F[int](), pkg.F[T](), (F)(), (pkg.F)(): unwrap until the shape is stable
        if function.type == "index_expression":
            function = function.child_by_field_name("operand")
        else:
            function = function.named_child(0)
    if function is None:
        return
    if function.type == "identifier":
        name = _name(walk.content, function)
        if name is not None:
            walk.sites.append(_Site(walk.scope, "call", name))
    elif function.type == "selector_expression":
        # The operand may be the whole nested prefix of a chain: only look at it when it is a name.
        operand = function.child_by_field_name("operand")
        field_node = function.child_by_field_name("field")
        if operand is None or field_node is None or operand.type != "identifier":
            return
        qualifier = _name(walk.content, operand)
        name = _name(walk.content, field_node)
        if qualifier is not None and name is not None:
            walk.sites.append(_Site(walk.scope, "qcall", name, qualifier))


def _enter_instantiated_call(walk: _Walk, node: Node) -> None:
    if walk.skip_node != -1 or len(walk.sites) >= _MAX_SITES:
        return
    package, name = _type_name(walk.content, node.child_by_field_name("type"))
    if name is None:
        return
    if package is None:
        walk.sites.append(_Site(walk.scope, "call", name))
    else:
        walk.sites.append(_Site(walk.scope, "qcall", name, package))


def _enter_top(walk: _Walk, node: Node) -> None:
    kind = node.type
    if kind == "const_declaration":
        walk.const_tail = ([], [])
    if kind in ("function_declaration", "method_declaration"):
        _enter_function(walk, node)
    elif node.is_error or node.is_missing:
        walk.tainted += 1


_INTERFACE_PATH: Final = ["type_declaration", "type_spec", "interface_type"]
_STRUCT_PATH: Final = ["type_declaration", "type_spec", "struct_type", "field_declaration_list"]


def _enter_nested(walk: _Walk, node: Node, kind: str) -> None:
    if walk.skip_node != -1:
        return
    if kind == "call_expression":
        _enter_call(walk, node)
        return
    if kind == "type_conversion_expression":
        _enter_instantiated_call(walk, node)  # pkg.F[int](x) is parsed as a conversion
        return
    if len(walk.ancestors) > 5:
        return  # declarations sit at most this deep: never copy the path of a deep chain
    path = walk.ancestors[1:]  # below the source file
    if kind == "import_spec":
        if path in (["import_declaration"], ["import_declaration", "import_spec_list"]):
            _enter_import(walk, node)
    elif kind in ("type_spec", "type_alias"):
        if len(path) == 1 and path[0] == "type_declaration":
            _enter_type(walk, node)
    elif kind in ("const_spec", "var_spec"):
        holders = ("const_declaration", "var_declaration")
        if (len(path) == 1 and path[0] in holders) or path == [
            "var_declaration",
            "var_spec_list",
        ]:
            _enter_value_spec(walk, node)
    elif kind in ("type_elem", "method_elem"):
        if walk.type_sym is not None and path == _INTERFACE_PATH:
            _enter_interface_member(walk, node)
    elif kind == "field_declaration" and walk.type_sym is not None and path == _STRUCT_PATH:
        _enter_field(walk, node)


def _enter(walk: _Walk, node: Node) -> bool:
    """Emit the node's token and open declarations; True when its children must be walked."""
    kind = node.type
    walk.budget.node()
    if kind in _SKIPPED:
        return False
    depth = len(walk.ancestors)
    if depth == 1:
        _enter_top(walk, node)
    elif depth > 1:
        _enter_nested(walk, node, kind)
    for entry in walk.opens[-1:]:
        if entry.body_id == node.id:
            entry.header_end = len(walk.tokens)
        if node.id in entry.marks:
            entry.marks[node.id] = len(walk.tokens)
    if node.child_count == 0:
        walk.tokens.append(_leaf_token(walk.content, node))
        return False
    walk.tokens.append(frame(b"(" + kind.encode()))
    return True


def _finish_open(walk: _Walk, entry: _Open) -> None:
    end = len(walk.tokens)
    stop = entry.header_end if entry.header_end >= 0 else end
    if entry.const_mark >= 0:
        if entry.implicit:
            _finish_implicit_const(walk, entry)
            return
        after = entry.marks[entry.const_mark] + 1
        walk.const_tail = (walk.tokens[after:stop], walk.tokens[stop:end])
    if not entry.syms:
        return
    if entry.names:
        _finish_declarators(walk, entry, stop, end)
        return
    semantic = digest(_SEMANTIC_DOMAIN, walk.tokens[entry.tok_start : end])
    signature = digest(_SIGNATURE_DOMAIN, walk.tokens[entry.tok_start : stop])
    for sym in entry.syms:
        sym.semantic = semantic
        sym.signature_digest = signature


def _finish_implicit_const(walk: _Walk, entry: _Open) -> None:
    """Digest a bare ``B`` in ``A = iota; B``: it repeats the previous spec's type and value."""
    tokens = walk.tokens
    typ, value = walk.const_tail
    for sym, index in entry.owners or [(entry.syms[0], 0)]:
        at = entry.marks[entry.names[index] if entry.names else entry.const_mark]
        head = [tokens[at], *typ]
        sym.signature_digest = digest(_SIGNATURE_DOMAIN, head)
        sym.semantic = digest(_SEMANTIC_DOMAIN, [*head, *value])


def _finish_declarators(walk: _Walk, entry: _Open, stop: int, end: int) -> None:
    """Digest each name of ``var a, b T = x, y`` from its own name, the shared type and its value."""
    tokens = walk.tokens
    positions = [entry.marks[item] for item in entry.names]
    after_names = max(positions) + 1
    shared_type = tokens[after_names:stop]
    starts = [entry.marks[item] for item in entry.values]
    paired = len(entry.values) == len(entry.names)
    for sym, index in entry.owners:
        head = [tokens[positions[index]], *shared_type]
        if paired:
            last = index + 1 == len(starts)
            value = tokens[starts[index] : end if last else starts[index + 1]]
        else:
            value = tokens[stop:end]  # ``a, b = f()``: every name depends on the whole value
        sym.signature_digest = digest(_SIGNATURE_DOMAIN, head)
        sym.semantic = digest(_SEMANTIC_DOMAIN, [*head, *value])


def _exit(walk: _Walk, node: Node, opened: bool) -> None:
    if opened:
        walk.tokens.append(_CLOSE)
        walk.ancestors.pop()
    if walk.opens and walk.opens[-1].node_id == node.id:
        _finish_open(walk, walk.opens.pop())
    if node.id == walk.skip_node:
        walk.skip_node = -1
    if node.id == walk.scope_node:
        walk.scope, walk.scope_node = None, -1
    if node.type in ("type_spec", "type_alias"):
        walk.type_sym = None


def _traverse(tree: Tree, walk: _Walk) -> None:
    cursor = tree.walk()
    while True:
        node = cursor.node
        if node is None:
            return
        if _enter(walk, node) and cursor.goto_first_child():
            walk.ancestors.append(node.type)
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


@dataclass(eq=False)
class _Output:
    """References, relations and counters collected from the sites, all under their caps."""

    index: dict[int, int]
    module: int | None
    budget: Budget
    output: list[int]
    relations: list[StructuralRelation] = field(default_factory=list)
    references: list[ParsedReference] = field(default_factory=list)
    per_relation: dict[int | None, int] = field(default_factory=dict)
    per_reference: dict[int | None, int] = field(default_factory=dict)
    edges: set[tuple[int, int, str]] = field(default_factory=set)
    refs: set[tuple[str, int, int]] = field(default_factory=set)
    seen: set[tuple[int | None, str, str, str, str]] = field(default_factory=set)
    capped: int = 0

    def full(self, source: int | None) -> bool:
        limit = MAX_MODULE_REFERENCES if source is None else MAX_REFERENCES_PER_SYMBOL
        return self.per_reference.get(source, 0) >= limit

    def reference(
        self,
        source: int | None,
        kind: str,
        name: _Name,
        qualifier: _Name | None,
        alias: _Name | None = None,
    ) -> None:
        marker = (kind, name.start, name.end)
        first = (
            source,
            kind,
            qualifier.text if qualifier else "",
            name.text,
            alias.text if alias else "",
        )
        if marker in self.refs or first in self.seen:
            return
        cost = (
            len(name.text.encode())
            + (len(qualifier.text.encode()) if qualifier else 0)
            + (len(alias.text.encode()) if alias else 0)
        )
        if (
            self.full(source)
            or len(self.references) >= MAX_REFERENCES_PER_FILE
            or not self.budget.take_reference(cost, self.output)
        ):
            self.capped += 1
            return
        self.refs.add(marker)
        self.seen.add(first)
        self.per_reference[source] = self.per_reference.get(source, 0) + 1
        self.references.append(
            ParsedReference(
                source=None if source is None else str(source),
                kind="call" if kind == "call" else "import" if kind == "import" else "inherit",
                target_name=name.text,
                start_byte=name.start,
                end_byte=name.end,
                qualifier=qualifier.text if qualifier else None,
                qualifier_start_byte=qualifier.start if qualifier else None,
                qualifier_end_byte=qualifier.end if qualifier else None,
                alias=alias.text if alias else None,
                alias_start_byte=alias.start if alias else None,
                alias_end_byte=alias.end if alias else None,
                evidence_kind=EVIDENCE_KIND,
                confidence="heuristic" if kind == "call" else "syntactic",
            )
        )

    def relation(self, source: int, target: int, kind: str, name: _Name) -> None:
        key = (source, target, kind)
        if key in self.edges or self.per_relation.get(source, 0) >= MAX_RELATIONS_PER_SYMBOL:
            return
        if len(self.relations) >= MAX_RELATIONS_PER_FILE:
            return
        self.edges.add(key)
        self.per_relation[source] = self.per_relation.get(source, 0) + 1
        self.relations.append(
            StructuralRelation(
                source_ref=str(source),
                target_ref=str(target),
                kind="inherits" if kind == "base" else "calls",
                start_byte=name.start,
                end_byte=name.end,
                evidence_kind=EVIDENCE_KIND,
            )
        )


def _resolve(walk: _Walk, result: _Output) -> None:
    """Same-file candidates by name (dict indexes); ambiguous names (over 8) give nothing."""
    functions: dict[str, list[_Sym]] = {}
    methods: dict[tuple[str, str], list[_Sym]] = {}
    types: dict[str, list[_Sym]] = {}
    for sym in walk.syms:
        if sym.kind == "function":
            functions.setdefault(sym.simple, []).append(sym)
        elif sym.kind == "method" and sym.recv_type:
            methods.setdefault((sym.recv_type, sym.simple), []).append(sym)
        elif sym.kind in ("class", "interface", "type") and sym.qualified == _join(
            walk.module_q, sym.simple
        ):
            types.setdefault(sym.simple, []).append(sym)
    for item in walk.imports:
        result.reference(None, "import", item.name, item.qualifier, item.alias)
    memo: dict[tuple[int, str, str], list[_Sym] | None] = {}
    for site in walk.sites:
        scope = site.scope
        source = result.module if scope is None else result.index.get(id(scope))
        if scope is not None and source is None:
            continue
        reference_source = None if scope is None else source
        if site.kind == "qbase":
            result.reference(reference_source, "inherit", site.name, site.qualifier)
            continue
        if site.kind == "qcall":
            assert site.qualifier is not None
            if scope is not None and scope.recv_name == site.qualifier.text and scope.recv_type:
                found = methods.get((scope.recv_type, site.name.text), [])
                pool: list[_Sym] | None = found if len(found) <= _MAX_CANDIDATES else None
                if not found:  # maybe declared in another file of the package
                    result.reference(reference_source, "call", site.name, site.qualifier)
                    continue
            elif site.qualifier.text in walk.import_names:
                result.reference(reference_source, "call", site.name, site.qualifier)
                continue
            else:
                continue
        else:
            table = types if site.kind == "base" else functions
            key = (id(scope), site.kind, site.name.text)
            if key not in memo:
                found = table.get(site.name.text, [])
                memo[key] = found if len(found) <= _MAX_CANDIDATES else None
            pool = memo[key]
            if pool is not None and not pool and site.name.text not in _PREDECLARED:
                kind = "call" if site.kind == "call" else "inherit"
                result.reference(reference_source, kind, site.name, None)
                continue
        if source is None or pool is None:
            continue
        for target in pool:
            result.relation(source, result.index[id(target)], site.kind, site.name)


def _parse_file(source: SourceFile, budget: Budget, output: list[int]) -> ParsedFile:
    content = source.content()
    empty = ParsedFile(
        path=source.path, language=source.language, parser_fingerprint=FINGERPRINT, symbols=()
    )
    if not content:
        return empty
    # A leading BOM is legal Go but not grammar: blank it (same length) so offsets stay raw.
    text = b"   " + content[3:] if content.startswith(_BOM) else content
    tree = bounded_parse(_PARSER, text, budget)
    package, clause = _package_name(tree.root_node, content)
    if clause is not None and package is None:
        return degraded_file(source, FINGERPRINT, "symbols_dropped")
    name = _module_qualified(source.path, package)
    root = _Sym(name.rsplit(".", 1)[-1], name, "module", 0, len(content))
    if clause is not None:
        root.signature = _header(content, clause.start_byte, clause.end_byte)
    walk = _Walk(content, budget, output, root, name)
    if name and not walk.add(root):
        # The module symbol was refused (name or output limit): the file's structure is lost.
        return degraded_file(source, FINGERPRINT, "symbols_dropped")
    _traverse(tree, walk)
    if name:
        root.semantic = digest(_SEMANTIC_DOMAIN, walk.tokens)
        root.signature_digest = digest(_SIGNATURE_DOMAIN, [frame(name.encode())])
    index = {id(sym): number for number, sym in enumerate(walk.syms)}
    result = _Output(index, index.get(id(root)), budget, output)
    _resolve(walk, result)
    result.capped += walk.oversized
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
        for number, sym in enumerate(walk.syms)
    )
    counts = (
        ("syntax_recovered", 1 if tree.root_node.has_error else 0),
        ("symbols_dropped", walk.dropped),
        ("references_capped", result.capped),
    )
    return ParsedFile(
        path=source.path,
        language=source.language,
        parser_fingerprint=FINGERPRINT,
        symbols=symbols,
        relations=tuple(result.relations),
        references=tuple(result.references),
        diagnostics=tuple(
            ParsedDiagnostic(code=code, count=min(count, 1000))  # type: ignore[arg-type]
            for code, count in counts
            if count
        ),
    )


def parse_go(request: ParseRequest) -> ParsedModule:
    """Child-side entry: one ``ParsedFile`` per input file; the parent validates the result."""
    return safe_module(request, FINGERPRINT, _parse_file)


def go_adapter(
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
    sys.exit(serve(parse_go))
