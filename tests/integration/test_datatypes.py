"""Wire-level datatype coverage.

Exercises types that the CA / PVA backend has special handling for —
mbbi/mbbo enums, NTEnum, char waveforms, large double waveforms — so
a regression in the type dispatch surfaces here rather than as a
silently-wrong value much later.
"""

from __future__ import annotations

import time

import pytest


# ---------- CA mbbi/mbbo (enum) ----------


def test_ca_mbbi_enum_strs_present(ca_ctx):
    """ImageMode_RBV is mbbi — value is index, char_value is label,
    enum_strs is the full label list (16 entries with trailing empties
    per CA convention)."""
    pv = ca_ctx.create_pv("mini:dot:cam1:ImageMode_RBV")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    assert isinstance(r["value"], int)
    assert r["char_value"] in ("Single", "Multiple", "Continuous")
    enum_strs = r["enum_strs"]
    assert enum_strs[:3] == ("Single", "Multiple", "Continuous")
    # Value index round-trips against the label list.
    assert enum_strs[r["value"]] == r["char_value"]


def test_ca_mbbo_put_int_index(ca_ctx):
    """Putting an int index (0/1/2) sets the mode and the readback's
    int value reflects it. Asserts on the raw `value` rather than
    `char_value` because enum_strs is currently not re-projected onto
    the metadata dict after the first DBR_CTRL prefetch (tracked
    separately — char_value would correctly be 'Multiple' if the
    enum cache survived).

    Polls for RBV convergence rather than sleeping a fixed window —
    when other tests have left the cam1 in mid-acquisition the
    driver's state machine takes longer to propagate ImageMode → RBV.
    """
    sp = ca_ctx.create_pv("mini:dot:cam1:ImageMode")
    rbv = ca_ctx.create_pv("mini:dot:cam1:ImageMode_RBV")
    assert sp.wait_for_connection(timeout=3.0)
    assert rbv.wait_for_connection(timeout=3.0)

    def _wait_rbv(want: int, timeout: float = 2.0) -> int:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = rbv.get_with_metadata(timeout=1.0)["value"]
            if last == want:
                return last
            time.sleep(0.05)
        return last  # type: ignore[return-value]

    sp.put(1, wait=True, timeout=2.0)  # Multiple
    assert _wait_rbv(1) == 1

    sp.put(0, wait=True, timeout=2.0)  # back to Single (default)
    assert _wait_rbv(0) == 0


# ---------- PVA NTEnum ----------


def test_pva_ntenum_kohzu_mode(pva_ctx):
    """KohzuModeBO over PVA presents as NTEnum: value is the int
    index, char_value is the label, enum_strs lists the choices."""
    pv = pva_ctx.create_pv("mini:KohzuModeBO")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    assert r["enum_strs"] == ("Manual", "Auto")
    assert r["value"] in (0, 1)
    assert r["char_value"] == r["enum_strs"][r["value"]]


def test_pva_ntenum_put_index(pva_ctx):
    """Round-trip via the synchronous `pv.put(int)` path.

    PVA NTEnum requires writing to the `value.index` field; plain
    string-form pvput is silently rejected by the server because the
    top-level value is a structure, not a scalar. The wrapper detects
    NTEnum from the first read (sync OR async — see
    test_async_pva_ntenum_via_ophyd_async_strict_enum) and routes int / bool
    puts via `pvput_field("value.index", ...)`. The same routing now
    also runs in `put_async` / `put_nowait_async` so ophyd-async
    `await sig.set(MyEnum.X)` works."""
    pv = pva_ctx.create_pv("mini:KohzuModeBO")
    assert pv.wait_for_connection(timeout=3.0)
    # First get populates the NTEnum cache used by the put-routing.
    initial = pv.get_with_metadata(timeout=2.0)["char_value"]
    target = "Auto" if initial == "Manual" else "Manual"
    target_idx = 1 if target == "Auto" else 0

    pv.put(target_idx, wait=True, timeout=2.0)
    time.sleep(0.3)
    assert pv.get_with_metadata(timeout=2.0)["char_value"] == target

    # Restore initial state so following tests start from a known mode.
    initial_idx = 0 if initial == "Manual" else 1
    pv.put(initial_idx, wait=True, timeout=2.0)


def test_ca_mbbi_enum_strs_persist_across_reads(ca_ctx):
    """First DBR_CTRL prefetch carries enum_strs; subsequent DBR_TIME
    reads do not. The wrapper caches CTRL fields at the first read and
    re-injects them, so char_value resolves to the label string on
    every subsequent get_with_metadata call (not just the first).
    Without the cache, the second call's char_value would degrade to
    the raw int index."""
    pv = ca_ctx.create_pv("mini:dot:cam1:ImageMode_RBV")
    assert pv.wait_for_connection(timeout=3.0)

    first = pv.get_with_metadata(timeout=2.0)
    second = pv.get_with_metadata(timeout=2.0)
    third = pv.get_with_metadata(timeout=2.0)

    # All three must report the label, not just str(int).
    assert first["char_value"] in ("Single", "Multiple", "Continuous")
    assert second["char_value"] == first["char_value"]
    assert third["char_value"] == first["char_value"]
    # enum_strs must be present on every call.
    for r in (first, second, third):
        assert r.get("enum_strs", (None,))[0:3] == (
            "Single",
            "Multiple",
            "Continuous",
        ), f"missing/short enum_strs on a repeated read: {r.get('enum_strs')}"


def test_ca_units_persist_across_reads(ca_ctx):
    """Same cache mechanism: units / precision / display limits come
    from DBR_CTRL only. They must stay in the per-call dict on every
    subsequent DBR_TIME read."""
    pv = ca_ctx.create_pv("mini:current")
    assert pv.wait_for_connection(timeout=3.0)
    first = pv.get_with_metadata(timeout=2.0)
    second = pv.get_with_metadata(timeout=2.0)
    assert first["units"] == "mA"
    assert second["units"] == "mA"
    assert first["precision"] == second["precision"]


# ---------- CA char-typed string records ----------


def test_ca_stringin_value(ca_ctx):
    """Manufacturer_RBV is a stringin — value comes back as a Python
    str, not a byte array."""
    pv = ca_ctx.create_pv("mini:dot:cam1:Manufacturer_RBV")
    assert pv.wait_for_connection(timeout=3.0)
    r = pv.get_with_metadata(timeout=2.0)
    assert isinstance(r["value"], str)
    assert r["value"] == "Mini Beamline"


# ---------- Large CA double waveform ----------


def test_ca_large_int16_waveform(ca_ctx):
    """image1:ArrayData is a 307200-element Int16 (FTVL=SHORT)
    waveform. After enabling the image1 plugin's callbacks, triggering
    a single acquire, and waiting for propagation, the waveform should
    carry the full frame.

    image1:EnableCallbacks defaults to Disable, so the plugin would
    otherwise leave its array empty regardless of what cam1 publishes.
    """
    cb = ca_ctx.create_pv("mini:dot:cam1:ArrayCallbacks")
    en = ca_ctx.create_pv("mini:dot:image1:EnableCallbacks")
    mode = ca_ctx.create_pv("mini:dot:cam1:ImageMode")
    atime = ca_ctx.create_pv("mini:dot:cam1:AcquireTime")
    acquire = ca_ctx.create_pv("mini:dot:cam1:Acquire")
    img = ca_ctx.create_pv("mini:dot:image1:ArrayData")
    img_counter = ca_ctx.create_pv("mini:dot:image1:ArrayCounter_RBV")
    for p in (cb, en, mode, atime, acquire, img, img_counter):
        assert p.wait_for_connection(timeout=3.0)

    cb.put(1, wait=True, timeout=2.0)   # cam1: ArrayCallbacks on
    en.put(1, wait=True, timeout=2.0)   # image1: Enable
    mode.put(0, wait=True, timeout=2.0)  # Single
    atime.put(0.05, wait=True, timeout=2.0)

    # `acquire.put(1, wait=True)` returns as soon as the bo record is
    # processed and the driver has scheduled the acquisition — NOT
    # when the frame has actually arrived at the image1 plugin. Two
    # AreaDetector races to ride out together:
    #   1. ArrayCounter_RBV bumps when the plugin RECEIVES the NDArray
    #   2. ArrayData populates only after processCallbacks finishes
    #      copying the NDArray into the array param
    # Polling counter alone leaves a window where ArrayData is still
    # empty (CI runners are slow enough to land in it). Poll BOTH —
    # break only when the counter advanced AND the array is settled.
    before = img_counter.get_with_metadata(timeout=2.0)["value"]
    acquire.put(1, wait=True, timeout=5.0)

    deadline = time.time() + 5.0
    r = None
    last_len: int | str = "(no read)"
    after = before
    while time.time() < deadline:
        after = img_counter.get_with_metadata(timeout=2.0)["value"]
        if after > before:
            snap = img.get_with_metadata(timeout=2.0)
            if snap is not None:
                last_len = len(snap["value"])
                if last_len == 307200:
                    r = snap
                    break
        time.sleep(0.05)
    if r is None:
        pytest.fail(
            f"image1 didn't deliver a full 307200-element frame within 5 s "
            f"(counter {before}→{after}, last array length: {last_len})"
        )

    print(f"\n  image waveform: len={len(r['value'])}")

    assert len(r["value"]) == 307200
    # Pixel values are Int16 (poisson background ~1000 + Gaussian peak)
    # so values fit in int range and are non-negative.
    assert all(0 <= int(v) <= 65535 for v in r["value"][:100])


# ---------- CA vs PVA value parity on the same PV ----------


def test_ca_and_pva_serve_same_value(ca_ctx, pva_ctx):
    """mini:current is exposed via both CA and PVA. The two readings
    should agree to within one beam-current update interval (100 ms
    × ~25 mA/s slope ≈ 2.5 mA tolerance, plus some PVA timestamp
    skew)."""
    ca_pv = ca_ctx.create_pv("mini:current")
    pva_pv = pva_ctx.create_pv("mini:current")
    assert ca_pv.wait_for_connection(timeout=3.0)
    assert pva_pv.wait_for_connection(timeout=3.0)

    # Read in quick succession to minimise drift.
    v_ca = ca_pv.get_with_metadata(timeout=2.0)["value"]
    v_pva = pva_pv.get_with_metadata(timeout=2.0)["value"]
    print(f"\n  CA={v_ca:.3f}  PVA={v_pva:.3f}")
    # Both must be within the OFFSET ± AMPLITUDE band.
    assert 400 < v_ca < 600
    assert 400 < v_pva < 600
    # Within one update interval at peak slope: |Δ| ≤ 5 mA.
    assert abs(v_ca - v_pva) < 5.0
