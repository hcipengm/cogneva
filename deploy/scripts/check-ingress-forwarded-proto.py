#!/usr/bin/env python3
"""A route that rewrites X-Forwarded-Proto must rewrite it to the scheme it serves.

Why this is a gate: the header is what the application reads to build absolute
addresses -- the A2A card's `url` is the one that bites. A synthetic test can set
the header to anything and see the application agree, so the test cannot catch a
manifest that lies: it feeds the value the proxy would have fed if the proxy were
configured the other way. The lie is invisible on every surface the tree has,
and it is invisible in production too until a peer is handed a URL it cannot
reach.

What this judges: for each Ingress, the scheme the route actually serves
(`websecure` in the entrypoints annotation, or a TLS section) is compared with
the `X-Forwarded-Proto` value any Middleware that route names would set. They
must agree. A route that rewrites nothing passes -- the proxy's own value is
then the truth.

Exit codes: 0 every rewrite matches its route; 1 at least one does not (each
named with both schemes); 3 no route could be read -- which is not 0, because
"nothing violated" and "nothing was judged" must not look the same.

Usage: check-ingress-forwarded-proto.py [deploy/k3s] [deploy/rendered/*]
"""

from __future__ import annotations

import glob
import os
import sys

import yaml

HEADER = "X-Forwarded-Proto"


def load_docs(directories):
    """Yield (where, document) for every YAML document in the given directories."""
    for directory in directories:
        for path in sorted(glob.glob(os.path.join(directory, "*.yaml"))):
            with open(path, encoding="utf-8") as fh:
                try:
                    docs = [d for d in yaml.safe_load_all(fh) if isinstance(d, dict)]
                except yaml.YAMLError as exc:
                    raise OSError(f"cannot parse {path}: {exc}") from exc
            for i, doc in enumerate(docs):
                yield (f"{os.path.basename(directory)}/{os.path.basename(path)}#{i}", doc)


def route_scheme(ingress):
    """The scheme a client's request arrives on, as the route is configured."""
    annotations = (ingress.get("metadata") or {}).get("annotations") or {}
    entrypoints = str(annotations.get("traefik.ingress.kubernetes.io/router.entrypoints", ""))
    if "websecure" in [p.strip() for p in entrypoints.split(",") if p.strip()]:
        return "https"
    if (ingress.get("spec") or {}).get("tls"):
        return "https"
    return "http"


def middleware_names(ingress):
    annotations = (ingress.get("metadata") or {}).get("annotations") or {}
    raw = str(annotations.get("traefik.ingress.kubernetes.io/router.middlewares", ""))
    names = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        # `namespace-name@kubernetescrd`
        names.append(item.split("@")[0])
    return names


def rewrite_of(middleware):
    """The value this middleware would force into the request header, or None.

    Request headers only: that is the copy the application reads when it builds
    an absolute address. A `customResponseHeaders` entry of the same name is
    emitted towards the client and never reaches the handler, so judging it here
    would red on a manifest that lies to nobody.
    """
    spec = middleware.get("spec") or {}
    headers = spec.get("headers") or {}
    values = headers.get("customRequestHeaders") or {}
    if isinstance(values, dict) and HEADER in values:
        return str(values[HEADER])
    return None


def judge(directory, docs):
    """Judge one directory's routes against that directory's own middlewares.

    Per directory, never across them: each rendered profile and the k3s static
    baseline are separately deployable sets, and a middleware that only one of
    them still carries would otherwise be found by the others' routes and the
    divergence would read as agreement.
    """
    # Two keys per middleware, because traefik's CRD provider is referenced as
    # `<namespace>-<name>@kubernetescrd` while the object itself carries the two
    # parts apart. Reading only `metadata.name` would call every real reference
    # unresolved -- which is why the unresolved list is printed rather than
    # silently folded into "no rewrites here".
    middlewares = {}
    for where, doc in docs:
        if doc.get("kind") != "Middleware":
            continue
        meta = doc.get("metadata") or {}
        name = meta.get("name")
        if not name:
            continue
        namespace = meta.get("namespace")
        if namespace:
            middlewares[f"{namespace}/{name}"] = (where, doc)
            middlewares.setdefault(f"*{namespace}-{name}", (where, doc))
        middlewares.setdefault(f"*/{name}", (where, doc))

    def resolve(namespace, ref):
        return (
            middlewares.get(f"{namespace}/{ref}")
            or middlewares.get(f"*{ref}")
            or middlewares.get(f"*/{ref}")
        )

    routes = 0
    rewrites = 0
    violations = []
    unknown = []
    for where, doc in docs:
        if doc.get("kind") != "Ingress":
            continue
        routes += 1
        scheme = route_scheme(doc)
        meta = doc.get("metadata") or {}
        name = meta.get("name", "?")
        namespace = meta.get("namespace")
        for mw in middleware_names(doc):
            found = resolve(namespace, mw)
            if found is None:
                unknown.append((where, name, mw))
                continue
            value = rewrite_of(found[1])
            if value is None:
                continue
            rewrites += 1
            if value != scheme:
                violations.append((where, name, mw, scheme, value))
    return routes, rewrites, violations, unknown


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("-")]
    for a in argv[1:]:
        if a in ("-h", "--help"):
            print(__doc__)
            return 0
        if a.startswith("-"):
            print(f"check-ingress-forwarded-proto: unknown argument {a}", file=sys.stderr)
            return 2
    directories = args or ["deploy/k3s"] + sorted(glob.glob("deploy/rendered/*"))
    directories = [d for d in directories if os.path.isdir(d)]

    routes = 0
    rewrites = 0
    violations = []
    unknown = []
    unreadable = 0
    for directory in directories:
        try:
            docs = list(load_docs([directory]))
        except OSError as exc:
            print(f"check-ingress-forwarded-proto: cannot read a manifest: {exc}", file=sys.stderr)
            print(
                "check-ingress-forwarded-proto: that directory was judged by nothing, "
                "which is not the same as clean",
                file=sys.stderr,
            )
            unreadable += 1
            continue
        r, w, v, u = judge(directory, docs)
        routes += r
        rewrites += w
        violations.extend(v)
        unknown.extend(u)

    for where, name, mw, scheme, value in violations:
        print(
            f"RED  {where} ingress/{name}: serves {scheme} but middleware {mw} "
            f"rewrites {HEADER} to {value}"
        )
        print(
            "     repair: either serve the route on the other entrypoint, or stop "
            "rewriting the header -- the application builds absolute addresses from it, "
            "and a peer handed the wrong scheme gets a URL it cannot reach"
        )
    for where, name, mw in unknown:
        print(f"CENSUS unresolved {where} ingress/{name}: names middleware {mw} with no body here")
    print(
        f"check-ingress-forwarded-proto: directories={len(directories)} routes={routes} "
        f"rewrites={rewrites} unresolved={len(unknown)} violations={len(violations)}"
    )
    if routes == 0:
        print(
            "check-ingress-forwarded-proto: no Ingress was read; nothing was judged",
            file=sys.stderr,
        )
        return 3
    if violations:
        return 1
    if unreadable:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
