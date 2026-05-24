"""Regenerate the committed sample option chain used for tests and the preview.

Writes an OTM-blend chain priced under a known SVI smile to
``tests/fixtures/options_sample.csv``. Uses far-future expiries so the fixture
stays valid (non-negative tenors) for years without regeneration.

    .devvenv/bin/python scripts/make_sample_chain.py
"""
import os
import sys

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

from tests._synth import synth_option_chain  # noqa: E402

OUT = os.path.join(_HERE, "tests", "fixtures", "options_sample.csv")
# Evergreen quarterly expiries (kept well in the future on purpose).
EXPIRIES = ["2030-03-15", "2030-06-21", "2030-09-20", "2030-12-20", "2031-06-20"]
BASE = "2026-01-02"


def main() -> None:
    df = synth_option_chain(EXPIRIES, BASE, spot=100.0, n_strikes=25,
                            rho=-0.4, eta=1.2, gamma=0.35, sigma_atm=0.22)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"wrote {len(df)} rows x {df['expiry'].nunique()} expiries -> {OUT}")


if __name__ == "__main__":
    main()
