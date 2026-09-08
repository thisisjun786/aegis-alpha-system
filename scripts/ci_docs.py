"""Check live Markdown links and changed prose whitespace, without dependencies."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

from scripts.ci_changes import ARCHIVE_PREFIXES, changed_paths, git


def links(text: str) -> list[str]:
    prose = re.sub(r"(?ms)^\s*(`{3,}|~{3,}).*?^\s*\1\s*$", "", text)
    prose = re.sub(r"`[^`\n]*`", "", prose)
    inline = re.findall(r"\[[^\]\n]*\]\((<[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)", prose)
    references = re.findall(r"(?m)^\s*\[[^\]]+\]:\s*(<[^>]+>|\S+)", prose)
    return [link.strip("<>") for link in inline + references]


def anchors(text: str) -> set[str]:
    counts: dict[str, int] = {}
    result: set[str] = set()
    for heading in re.findall(r"(?m)^#{1,6}\s+(.+?)\s*#*\s*$", text):
        slug = re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")
        index = counts.get(slug, 0)
        counts[slug] = index + 1
        result.add(f"{slug}-{index}" if index else slug)
    result.update(re.findall(r'(?:id|name)=["\x27]([^"\x27]+)["\x27]', text))
    return result


def check_documents(base: str, head: str, *, root: Path = Path()) -> list[str]:
    root = root.resolve()
    paths = changed_paths(base, head, root=root)
    changed_paths_md = [
        path for path in paths if path.endswith(".md") and not path.startswith(ARCHIVE_PREFIXES)
    ]
    if changed_paths_md:
        git("diff", "--check", base, head, "--", *changed_paths_md, root=root)
    changed = {root / path for path in changed_paths_md}
    # Validate incoming links too: a changed heading or deleted non-Markdown
    # target can break an unchanged live document. Historical snapshots are
    # preserved byte-for-byte; dev-notes/AGENTS.md gives them no live authority.
    documents = {
        root / path
        for path in git("ls-files", "-z", "*.md", root=root).decode().split("\0")
        if path and not path.startswith(ARCHIVE_PREFIXES) and (root / path).exists()
    }
    errors: list[str] = []
    for document in sorted(documents):
        text = document.read_text(encoding="utf-8")
        if document in changed and re.search(r"(?m)^(?:<{7}|={7}|>{7})(?: |$)", text):
            errors.append(f"{document.relative_to(root)}: conflict marker")
        for link in links(text):
            parsed = urlsplit(link)
            if parsed.scheme or parsed.netloc:
                continue
            target = (document.parent / unquote(parsed.path)).resolve() if parsed.path else document
            if not target.exists():
                errors.append(f"{document.relative_to(root)}: missing link {link}")
            elif (
                parsed.fragment
                and target.suffix == ".md"
                and unquote(parsed.fragment) not in anchors(target.read_text(encoding="utf-8"))
            ):
                errors.append(f"{document.relative_to(root)}: missing anchor {link}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()
    try:
        errors = check_documents(args.base, args.head)
    except subprocess.CalledProcessError as exc:
        raise SystemExit((exc.stdout + exc.stderr).decode("utf-8", errors="replace")) from exc
    if errors:
        raise SystemExit("\n".join(errors))
    print("documentation links, whitespace and conflict markers: pass")  # noqa: T201


if __name__ == "__main__":
    main()
