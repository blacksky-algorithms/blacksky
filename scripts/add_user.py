#!/usr/bin/env python3
import json
import os
import sys

import urllib.request

ENDING_BRACKET_DELIMITER = "\n]\n"

MEMBERSHIP_TS = os.path.join(os.path.dirname(sys.argv[0]), "..", "src", "membership.ts")


def resolve_did(did):
    url = f"https://plc.directory/{did}"
    try:
        json_payload = urllib.request.urlopen(url).read()
    except urllib.error.HTTPError as e:
        print(f"Failed to resolve DID {did}: {e}")
        sys.exit(1)

    doc = json.loads(json_payload)
    if "alsoKnownAs" not in doc:
        print(f"Failed to resolve DID {did}: {doc}")
        sys.exit(1)

    handle = doc["alsoKnownAs"][0].replace("at://", "")
    return handle, did


def resolve_handle(handle):
    if "." not in handle:
        handle = f"{handle}.bsky.social"

    url = (
        f"https://bsky.social/xrpc/com.atproto.identity.resolveHandle"
        f"?handle={handle}"
    )
    try:
        json_payload = urllib.request.urlopen(url).read()
    except urllib.error.HTTPError as e:
        print(f"Failed to resolve {handle}: {e}")
        sys.exit(1)

    doc = json.loads(json_payload)
    if "did" not in doc:
        print(f"Failed to resolve {handle}: {doc}")
        sys.exit(1)

    did = doc["did"]
    return handle, did


def main():
    if len(sys.argv) < 2:
        print("Usage: add_user.py <did_or_handle>")
        sys.exit(1)

    did_or_handle = sys.argv[1]
    if did_or_handle.startswith("did:plc:"):
        handle, did = resolve_did(did_or_handle)
    else:
        handle, did = resolve_handle(did_or_handle)

    print(f"handle = {handle}", file=sys.stderr)
    print(f"   did = {did}", file=sys.stderr)

    membership = open(MEMBERSHIP_TS).read()
    if did in membership:
        print(f"{handle} / {did} already in membership.ts")
        sys.exit(0)

    sections = membership.split(ENDING_BRACKET_DELIMITER)
    new_sections = []
    for section in sections:
        if "DID_ADDITIONS" in section:
            lines = section.split("\n")
            lines.append(f"  '{did}',")
            section = "\n".join(lines)
        new_sections.append(section)
    new_membership = ENDING_BRACKET_DELIMITER.join(new_sections)
    open(MEMBERSHIP_TS, "w").write(new_membership)
    print(f"Added {handle} / {did} to membership.ts")


if __name__ == "__main__":
    main()
