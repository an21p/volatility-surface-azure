import numpy as np
from conftest import load_module

analysis = load_module("volatility_surface/analysis.py", "analysis")


def test_coarse_grid_downsamples_and_scales():
    strikes = np.linspace(100, 200, 21)
    tenors = np.linspace(0.0, 1.5, 50)
    vol = np.full((50, 21), 0.20)  # 20% in decimal
    g = analysis.coarse_grid(strikes, tenors, vol, n=5)
    assert len(g["strikes"]) == 5
    assert len(g["tenors"]) == 5
    assert len(g["iv"]) == 5 and len(g["iv"][0]) == 5
    assert g["iv"][0][0] == 20.0          # scaled to percent
    assert g["strikes"][0] == 100.0 and g["strikes"][-1] == 200.0
