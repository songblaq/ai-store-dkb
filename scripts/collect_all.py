"""Collect all sources defined in config/sources.json."""

from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    config_path = Path(__file__).parent.parent / "config" / "sources.json"
    with open(config_path) as f:
        config = json.load(f)

    categories = config.get("categories", {})
    total = sum(len(sources) for sources in categories.values())

    print(f"ai-store-dkb collector")
    print(f"Categories: {len(categories)}")
    print(f"Total sources: {total}")
    print()

    for category, sources in categories.items():
        print(f"[{category}] {len(sources)} sources")
        for source in sources:
            print(f"  - {source['label']} ({source['provenance_hint']})")
            print(f"    {source['origin_uri']}")

    print()
    print("TODO: Implement actual collection using dkb_runtime.services.collector")


if __name__ == "__main__":
    main()
