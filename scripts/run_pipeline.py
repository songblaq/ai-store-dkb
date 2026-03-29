"""Run the full DKB pipeline: collect -> extract -> canonicalize -> score -> verdict."""

from __future__ import annotations


def main() -> None:
    print("ai-store-dkb pipeline")
    print()
    print("Steps:")
    print("  1. Collect sources (git clone/fetch)")
    print("  2. Extract raw directives")
    print("  3. Canonicalize (deduplicate, normalize)")
    print("  4. Score dimensions (DG)")
    print("  5. Generate verdicts")
    print()
    print("TODO: Implement using dkb_runtime services")


if __name__ == "__main__":
    main()
