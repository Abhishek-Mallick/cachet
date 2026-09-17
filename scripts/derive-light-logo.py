#!/usr/bin/env python3
"""Derive the light-background lockup from the dark-background one.

The logo is a teal gradient mark next to a wordmark. The mark reads on any ground; the wordmark is
white, so on a light page it disappears. Rather than hand-maintain two exports that drift, the light
variant is generated from the original by recolouring only the achromatic pixels.

Achromatic is the whole test: the mark is teal everywhere, so a low-chroma, high-luminance pixel can
only belong to the wordmark. RGB alone is rewritten and alpha is left untouched, so antialiasing at
the glyph edges survives and the result composites cleanly onto any background.

    python3 scripts/derive-light-logo.py

Requires Pillow and numpy. Run it when the source logo changes, and commit both outputs.
"""

import sys
from pathlib import Path

import numpy as np
from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "web" / "public" / "cachet-logo.png"
OUTPUTS = [
    ROOT / "web" / "public" / "cachet-logo-on-light.png",
    ROOT / ".github" / "assets" / "cachet-logo-on-light.png",
]

# Not pure black: #0a0a0a is the same ink the rest of the light theme uses for headings.
INK = (10, 10, 10)
MAX_CHROMA = 30  # Above this a pixel carries hue, so it belongs to the mark.
MIN_LUMA = 120  # Below this it is a shadow rather than the wordmark.


def main() -> int:
    a = np.array(Image.open(SOURCE).convert("RGBA")).astype(np.int16)
    rgb, alpha = a[..., :3], a[..., 3]

    chroma = rgb.max(axis=-1) - rgb.min(axis=-1)
    wordmark = (chroma < MAX_CHROMA) & (rgb.mean(axis=-1) > MIN_LUMA) & (alpha > 0)

    visible = int((alpha > 0).sum())
    found = int(wordmark.sum())
    if not 0.2 * visible < found < 0.8 * visible:
        # A wildly different share means the logo changed shape and the thresholds no longer
        # separate wordmark from mark. Failing here beats silently shipping a mangled lockup.
        print(f"refusing to write: {found} of {visible} visible pixels matched the wordmark test")
        return 1

    out = a.copy()
    out[wordmark, 0:3] = INK
    image = Image.fromarray(out.astype(np.uint8), "RGBA")
    for path in OUTPUTS:
        image.save(path, optimize=True)
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
