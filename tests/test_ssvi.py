"""Tests for the SSVI calibration module and the build_surface integration."""
from datetime import date, timedelta

import numpy as np
import pytest

from visualiser import ssvi
from tests._synth import synth_option_chain


# ---- SSVI formula sanity ----------------------------------------------------

def test_atm_total_variance_equals_theta():
    theta = 0.04
    w = ssvi.ssvi_total_variance(0.0, theta, -0.4, 1.2, 0.35)
    assert w == pytest.approx(theta)


def test_symmetric_smile_when_rho_zero():
    k = np.array([0.05, 0.1, 0.2, 0.3])
    theta = 0.05
    w_pos = ssvi.ssvi_total_variance(k, theta, 0.0, 1.0, 0.4)
    w_neg = ssvi.ssvi_total_variance(-k, theta, 0.0, 1.0, 0.4)
    assert w_pos == pytest.approx(w_neg)


def test_total_variance_nonnegative():
    k = np.linspace(-0.6, 0.6, 50)
    w = ssvi.ssvi_total_variance(k, 0.04, -0.7, 2.0, 0.4)
    assert (w >= 0).all()


def test_enforce_monotone():
    out = ssvi.enforce_monotone([0.04, 0.03, 0.05, 0.045, 0.06])
    assert (np.diff(out) >= 0).all()
    assert out[0] == 0.04


def test_butterfly_cap_enforced():
    # Deliberately large eta + total variances that would violate no-arbitrage.
    thetas = np.array([0.2, 0.5, 1.0])
    rho, gamma = -0.5, 0.2
    capped = ssvi.cap_eta_for_no_butterfly(thetas, rho, 50.0, gamma)
    a = 1.0 + abs(rho)
    for th in thetas:
        ph = ssvi.phi(th, capped, gamma)
        assert th * ph * a < 4.0 + 1e-9
        assert th * ph ** 2 * a <= 4.0 + 1e-9


# ---- Calibration recovers known parameters ----------------------------------

def test_calibration_recovers_params():
    rho, eta, gamma = -0.45, 1.3, 0.38
    sigma_atm = 0.22
    slices = []
    for t in (0.1, 0.25, 0.5, 1.0):
        theta = sigma_atm ** 2 * t
        k = np.linspace(-0.4, 0.4, 25)
        w = ssvi.ssvi_total_variance(k, theta, rho, eta, gamma)
        slices.append((t, k, w))

    p = ssvi.calibrate_ssvi(slices)

    assert p.rho == pytest.approx(rho, abs=0.05)
    # Term structure is non-decreasing (calendar-arbitrage-free).
    assert (np.diff(p.thetas) >= 0).all()
    # eta/gamma are only jointly identifiable, so the meaningful check is that
    # the calibrated surface reproduces the known total variances everywhere.
    for t, k, w in slices:
        theta = float(ssvi.theta_at(p, t))
        w_model = ssvi.ssvi_total_variance(k, theta, p.rho, p.eta, p.gamma)
        assert np.max(np.abs(w_model - w)) < 1e-3


def test_calibration_raises_without_slices():
    with pytest.raises(ValueError):
        ssvi.calibrate_ssvi([])


# ---- End-to-end through build_surface (needs QuantLib) ----------------------

def test_build_surface_smooth_and_skewed():
    import visualiser

    today = date.today()
    expiries = [(today + timedelta(days=d)).isoformat() for d in (30, 60, 120, 200)]
    df = synth_option_chain(expiries, today.isoformat(), spot=100.0, rho=-0.4)

    strikes, tenors, vol = visualiser.build_surface(df, "TEST")
    vol = np.asarray(vol)
    K = np.asarray(strikes, dtype=float)

    # Fixed moneyness window: 60 strikes x 50 tenors.
    assert vol.shape == (50, 60)
    assert np.isfinite(vol).all()
    assert (vol > 0.01).all() and (vol < 2.0).all()

    # Smoothness across strike: small second differences on a mid tenor row.
    row = vol[10]
    second_diff = np.abs(np.diff(row, 2))
    assert second_diff.max() < 0.02

    # Downside skew (rho < 0): 90%-moneyness IV exceeds 110%-moneyness IV.
    spot = float(df["spot"].iloc[0])
    lo_i = int(np.argmin(np.abs(K - 0.9 * spot)))
    hi_i = int(np.argmin(np.abs(K - 1.1 * spot)))
    assert vol[3, lo_i] > vol[3, hi_i]
