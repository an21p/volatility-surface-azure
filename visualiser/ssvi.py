"""SSVI (surface SVI) calibration for the implied-volatility surface.

Implements the Gatheral-Jacquier SSVI parameterization of total implied
variance. Unlike a per-strike interpolator, SSVI fits a single smooth surface
that is calendar-arbitrage-free by construction (given a non-decreasing ATM
variance term structure) and stays well-behaved when each expiry slice has only
a handful of quotes.

Total implied variance as a function of log-moneyness ``k = ln(K / F)``::

    w(k, theta) = (theta / 2) * [ 1 + rho*phi*k + sqrt((phi*k + rho)**2 + (1 - rho**2)) ]
    phi(theta)  = eta * theta**(-gamma)         # power-law curvature

with per-expiry ATM total variance ``theta_t = w(0, theta_t)`` and three global
parameters ``rho in (-1, 1)``, ``eta > 0``, ``gamma in (0, 0.5]``. Implied vol is
recovered as ``sigma(k, t) = sqrt(w / t)``.

This module is pure numpy/scipy (no QuantLib, no Azure), so it is unit-testable
in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

import numpy as np
from scipy.optimize import least_squares


@dataclass
class SSVIParams:
    """Calibrated SSVI surface.

    ``ts`` and ``thetas`` are the per-slice tenors (years) and ATM total
    variances, sorted by tenor and non-decreasing in ``thetas``. ``rho``,
    ``eta``, ``gamma`` are the global curvature/skew parameters.
    """
    ts: np.ndarray
    thetas: np.ndarray
    rho: float
    eta: float
    gamma: float


def phi(theta: np.ndarray | float, eta: float, gamma: float) -> np.ndarray | float:
    """Power-law curvature function ``phi(theta) = eta * theta**(-gamma)``."""
    theta = np.maximum(theta, 1e-12)
    return eta * theta ** (-gamma)


def ssvi_total_variance(k, theta, rho: float, eta: float, gamma: float):
    """Total implied variance ``w(k, theta)`` for the SSVI surface.

    ``k`` and ``theta`` broadcast against each other, so this evaluates a single
    slice (scalar ``theta``, vector ``k``) or a whole grid.
    """
    k = np.asarray(k, dtype=float)
    ph = phi(theta, eta, gamma)
    disc = (ph * k + rho) ** 2 + (1.0 - rho ** 2)
    return 0.5 * theta * (1.0 + rho * ph * k + np.sqrt(disc))


def estimate_atm_total_variance(k: np.ndarray, w: np.ndarray) -> float:
    """ATM total variance for one slice: total variance at ``k = 0``.

    Fits a local quadratic in log-moneyness when there are enough points,
    otherwise falls back to the observation nearest the money. Always positive.
    """
    k = np.asarray(k, dtype=float)
    w = np.asarray(w, dtype=float)
    if k.size >= 3 and np.ptp(k) > 1e-9:
        deg = 2 if k.size >= 3 else 1
        coeffs = np.polyfit(k, w, deg)
        atm = float(np.polyval(coeffs, 0.0))
    else:
        atm = float(w[np.argmin(np.abs(k))])
    return max(atm, 1e-8)


def enforce_monotone(thetas: Sequence[float]) -> np.ndarray:
    """Clamp ATM total variances to be non-decreasing (calendar-arb-free)."""
    return np.maximum.accumulate(np.asarray(thetas, dtype=float))


def cap_eta_for_no_butterfly(thetas: np.ndarray, rho: float, eta: float,
                             gamma: float) -> float:
    """Largest ``eta'`` <= ``eta`` satisfying the SSVI butterfly conditions.

    Gatheral-Jacquier give sufficient no-butterfly-arbitrage conditions
    ``theta*phi*(1+|rho|) < 4`` and ``theta*phi**2*(1+|rho|) <= 4``. Since ``phi``
    grows with ``eta`` and the longest expiry (largest ``theta``) binds first, we
    cap ``eta`` to the tightest per-slice bound.
    """
    thetas = np.asarray(thetas, dtype=float)
    a = 1.0 + abs(rho)
    caps = [eta]
    for th in thetas:
        th = max(th, 1e-12)
        # phi*theta*a < 4  -> eta < 4 * theta**(gamma-1) / a
        caps.append(4.0 * th ** (gamma - 1.0) / a)
        # phi**2*theta*a <= 4  -> eta <= theta**gamma * sqrt(4 / (theta*a))
        caps.append(th ** gamma * np.sqrt(4.0 / (th * a)))
    return float(max(min(caps) * (1.0 - 1e-6), 1e-6))


def calibrate_ssvi(slices: List[Tuple[float, np.ndarray, np.ndarray]]) -> SSVIParams:
    """Calibrate an SSVI surface from per-expiry ``(t, k, w)`` point sets.

    Two-stage and deliberately robust for sparse data:

    1. Estimate ATM total variance ``theta_t`` per slice, then clamp the term
       structure to be non-decreasing.
    2. Fit the three global parameters ``(rho, eta, gamma)`` by least squares
       against all observed ``(k, w)`` points with ``theta_t`` held fixed, then
       cap ``eta`` to respect the butterfly no-arbitrage bound.

    ``slices`` must contain at least one slice with >= 1 point. Raises
    ``ValueError`` otherwise so the caller can fall back.
    """
    usable = [(t, np.asarray(k, float), np.asarray(w, float))
              for (t, k, w) in slices
              if np.asarray(k, float).size >= 1 and t > 0]
    if not usable:
        raise ValueError("no usable slices for SSVI calibration")

    usable.sort(key=lambda s: s[0])
    ts = np.array([t for (t, _, _) in usable], dtype=float)
    thetas = enforce_monotone(
        [estimate_atm_total_variance(k, w) for (_, k, w) in usable])

    def residuals(p: np.ndarray) -> np.ndarray:
        rho, eta, gamma = p
        res = []
        for (theta, (_, k, w)) in zip(thetas, usable):
            model = ssvi_total_variance(k, theta, rho, eta, gamma)
            # Down-weight dense slices so one expiry can't dominate the fit.
            res.append((model - w) / np.sqrt(k.size))
        return np.concatenate(res)

    p0 = np.array([-0.5, 1.0, 0.4])
    lo = np.array([-0.999, 1e-6, 1e-3])
    hi = np.array([0.999, 100.0, 0.5])
    sol = least_squares(residuals, p0, bounds=(lo, hi), max_nfev=2000)
    rho, eta, gamma = sol.x

    eta = cap_eta_for_no_butterfly(thetas, rho, eta, gamma)
    return SSVIParams(ts=ts, thetas=thetas, rho=float(rho), eta=eta,
                      gamma=float(gamma))


def theta_at(params: SSVIParams, t: np.ndarray | float) -> np.ndarray | float:
    """ATM total variance at arbitrary tenor(s), interpolated in total variance.

    Linear interpolation in ``theta`` vs ``t`` preserves the non-decreasing term
    structure; out-of-range tenors clamp to the nearest calibrated slice.
    """
    return np.interp(t, params.ts, params.thetas)


def evaluate_surface(params: SSVIParams, strikes: np.ndarray, tenors: np.ndarray,
                     spot: float, r: float = 0.03, q: float = 0.01) -> np.ndarray:
    """Sample implied volatility (decimal) on a ``(tenor, strike)`` grid.

    Forward is ``F_t = spot * exp((r - q) * t)``; log-moneyness ``k = ln(K/F_t)``.
    Returns a ``(len(tenors), len(strikes))`` array of ``sigma = sqrt(w / t)``.
    """
    strikes = np.asarray(strikes, dtype=float)
    tenors = np.asarray(tenors, dtype=float)
    out = np.zeros((tenors.size, strikes.size))
    for i, t in enumerate(tenors):
        t = max(float(t), 1e-6)
        fwd = spot * np.exp((r - q) * t)
        k = np.log(strikes / fwd)
        theta = float(theta_at(params, t))
        w = ssvi_total_variance(k, theta, params.rho, params.eta, params.gamma)
        out[i, :] = np.sqrt(np.maximum(w, 0.0) / t)
    return out
