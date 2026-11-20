#!/usr/bin/env python3
"""A registry row that names a producer's path must fail at the move, not at the next person.

A contract table says "this series is published by that file". The row is checked
today by a Rust test that reads the file and looks for the name -- a real gate,
but one that only runs when someone runs that test. The person who moves the file
runs their own module's tests and sees nothing; the red lands on whoever runs the
contract test next, and on this host a red at the head of the mainline holds
every later revision, so the cost lands on strangers.

This asks the same question earlier and cheaper: for the commit about to be
made, every registered path must still be there and still contain the name it is
registered for. Milliseconds, no compile, no build slot.

Two directions, because either one alone lets a real accident through:
  - the path is gone (file moved, table not carried along) -- only the existence
    half sees it;
  - the path is still there but the name left it (the old file now re-exports
    from the new home) -- only the content half sees it. This is the worse of the
    two: the table reads fine and licenses bytes that are no longer the producer.

Judged against the index, not the working tree, whenever there is a repository
at all: the question is about the commit being made, and falling back to the file
on disk answers a different question -- a staged deletion leaves the file exactly
where it was, so the fallback turns the very accident this gate exists for into a
clean reading. Only a tree with no repository (a fixture, a tarball) is read from
disk, because there the files are the only answer there is.

Both halves of the question -- the table's rows and the files those rows name --
are read from that one source. Taking the rows off disk while taking their
targets out of the index mixes two questions, and the mix is not harmless: an
unstaged row a peer added to a table refuses a commit that does not carry the
table at all, which is the gate firing on bytes that are not before it.

What it cannot see: a semantic move that keeps both path and name and changes
what produces the value. That half stays with the Rust test.

Exit codes: 0 every registered producer path is where the table says and still
carries its name; 1 at least one row is an empty permit (named, with which
half failed); 3 no contract table could be read -- "nothing violated" and
"nothing was judged" must not look the same.

Usage: check-registered-producer-paths.py [--repo <path>] [--list]
"""

from __future__ import annotations

import glob
import os
import re
import subprocess
import sys

ARRAY = re.compile(r"const\s+PRODUCED\s*:\s*&\[\(&str,\s*&str\)\]\s*=\s*&\[")
PAIR = re.compile(r'\(\s*"((?:[^"\\]|\\.)*)"\s*,\s*"((?:[^"\\]|\\.)*)"\s*,?\s*\)', re.S)


TABLE_REL = re.compile(r"^crates/[^/]+/tests/[^/]*contract_test\.rs$")


def table_candidates(repo: str, has_git: bool) -> list:
    """Every path that could hold a contract table, from both sources of truth.

    The glob sees what is on disk and `git ls-files` sees what the commit
    carries. Both are needed: a table staged and then removed from the working
    tree is on disk no longer and still in the index, and a table a peer has
    just written but not staged is on disk and not in the index.
    """
    seen = set()
    for path in glob.glob(os.path.join(repo, "crates", "*", "tests", "*contract_test.rs")):
        seen.add(os.path.relpath(path, repo))
    if has_git:
        try:
            out = subprocess.run(
                ["git", "-C", repo, "ls-files", "-z", "--", "crates"],
                capture_output=True, check=False,
            )
        except OSError:
            out = None
        if out is not None and out.returncode == 0:
            for rel in out.stdout.decode("utf-8", "replace").split("\0"):
                if rel and TABLE_REL.match(rel):
                    seen.add(rel)
    return sorted(seen)


def table_texts(repo: str, has_git: bool):
    """Yield (relpath, text) for every contract table this commit declares PRODUCED in.

    The rows come from the same place as the files they name -- the commit's
    bytes. Reading the rows off the working tree while reading their targets
    out of the index asks two different questions at once, and a peer's unstaged
    edit to a row the commit does not carry then refuses a commit that has
    nothing to do with it.
    """
    for rel in table_candidates(repo, has_git):
        text = file_text(repo, rel, has_git)
        if text is None:
            continue
        if ARRAY.search(text):
            yield (rel, text)


def array_body(text: str, start: int) -> str:
    """The entries between the array's `= &[` and its matching `]`.

    Anchored on `= &[`, not on the first `&[`: the declared type is itself
    `&[(&str, &str)]`, and starting there closes on the type's own bracket,
    leaving an empty body that parses to zero rows -- a gate that judges
    nothing while reporting a clean count.
    """
    open_at = text.index("= &[", start) + len("= &[")
    depth = 1
    i = open_at
    while i < len(text):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return text[open_at:i]
        i += 1
    raise ValueError("unterminated PRODUCED array")


def rows(relpath: str, text: str):
    m = ARRAY.search(text)
    body = array_body(text, m.start())
    for name, where in PAIR.findall(body):
        yield (relpath, name, where)


def index_text(repo: str, rel: str):
    """The committed-or-staged bytes of `rel`, or None when the commit does not have it."""
    try:
        out = subprocess.run(
            ["git", "-C", repo, "show", f":{rel}"],
            capture_output=True, check=False,
        )
    except OSError:
        return None
    if out.returncode != 0:
        return None
    return out.stdout.decode("utf-8", "replace")


def git_available(repo: str) -> bool:
    try:
        out = subprocess.run(
            ["git", "-C", repo, "rev-parse", "--git-dir"],
            capture_output=True, check=False,
        )
    except OSError:
        return False
    return out.returncode == 0


def file_text(repo: str, rel: str, has_git: bool):
    """The bytes this commit holds for `rel`, or None.

    Where there is a repository, only the index counts: a row whose path the
    commit does not carry is an empty permit, and falling back to the file on
    disk would answer a different question than the one being asked (a staged
    deletion leaves the file exactly where it was). Where there is no repository
    at all -- a fixture, a tarball -- the files on disk are the only answer there
    is.
    """
    if has_git:
        return index_text(repo, rel)
    path = os.path.join(repo, rel)
    if os.path.exists(path):
        with open(path, encoding="utf-8", errors="replace") as fh:
            return fh.read()
    return None


METRIC_NAMES_REL = "crates/cog-core/src/contract/metric_names.rs"
REGISTRY_ROW = re.compile(r'^\s*([A-Z][A-Z0-9_]*)\s*=>\s*"([^"]+)",', re.M)


def registry_identifiers(repo: str, has_git: bool) -> dict:
    """name -> identifier, from the metric registry the contract table's second form names.

    A producer names its series either by quoting the literal or through this
    registry's constant. Both are what a rename moves, so both count -- and
    checking only the quoted form reports every registry-named series as
    missing its producer, which is a gate that would be switched off within a
    day.
    """
    text = file_text(repo, METRIC_NAMES_REL, has_git)
    if text is None:
        return {}
    return {name: ident for ident, name in REGISTRY_ROW.findall(text)}


def carries_the_producer(text: str, name: str, registry: dict) -> bool:
    if f'"{name}"' in text:
        return True
    ident = registry.get(name)
    if not ident:
        return False
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(ident)}(?![A-Za-z0-9_])", text) is not None


def main(argv):
    repo = ""
    list_only = False
    args = argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--repo":
            i += 1
            if i >= len(args):
                print("check-registered-producer-paths: --repo 少了路径", file=sys.stderr)
                return 2
            repo = args[i]
        elif a == "--list":
            list_only = True
        elif a in ("-h", "--help"):
            print(__doc__)
            return 0
        elif a.startswith("-"):
            print(f"check-registered-producer-paths: unknown argument {a}", file=sys.stderr)
            return 2
        else:
            repo = a
        i += 1
    repo = os.path.abspath(repo or os.environ.get("COGNEVA_REPO") or ".")
    if not os.path.isdir(repo):
        print(f"check-registered-producer-paths: {repo} 不是一个目录", file=sys.stderr)
        print("check-registered-producer-paths: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    has_git = git_available(repo)
    tables = []
    try:
        tables = list(table_texts(repo, has_git))
    except OSError as exc:
        print(f"check-registered-producer-paths: 读不了契约表：{exc}", file=sys.stderr)
        return 3
    if not tables:
        print(f"check-registered-producer-paths: {repo} 下没有一份声明 PRODUCED 的契约表", file=sys.stderr)
        print("check-registered-producer-paths: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    registry = registry_identifiers(repo, has_git)
    if not registry:
        print(
            f"check-registered-producer-paths: 读不到度量名册 {METRIC_NAMES_REL}"
            "（名字的第二种写法判不了）",
            file=sys.stderr,
        )
        print("check-registered-producer-paths: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    judged = 0
    unparsed = 0
    violations = []
    for relpath, text in tables:
        for where, name, target in rows(relpath, text):
            # FOREIGN rows carry prose in this column; only PRODUCED carries paths.
            if "/" not in target or " " in target:
                unparsed += 1
                if list_only:
                    print(f"skip  {where} {name} <- {target[:40]}")
                continue
            judged += 1
            if list_only:
                print(f"reg   {where} {name} <- {target}")
                continue
            body = file_text(repo, target, has_git)
            if body is None:
                violations.append((where, name, target, "path_gone"))
                continue
            if not carries_the_producer(body, name, registry):
                violations.append((where, name, target, "name_gone"))

    for where, name, target, kind in violations:
        if kind == "path_gone":
            print(f"RED  path_gone  {where}: {name} 登记在 {target}，这一笔里没有这个文件")
            print("     repair: 把这一行改到新家（同类行一起改），别留一个指向旧地址的许可")
        else:
            print(f"RED  name_gone  {where}: {name} 登记在 {target}，该文件里已经没有这个名字")
            print("     repair: 文件名还在不代表产出还在——转发/re-export 会让这张表读起来正常却认证别人的字节")
    if not list_only:
        print(
            f"check-registered-producer-paths: tables={len(tables)} judged={judged} "
            f"skipped_non_path={unparsed} violations={len(violations)}"
        )
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
