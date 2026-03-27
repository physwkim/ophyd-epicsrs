# ophyd-epicsrs

Rust EPICS Channel Access backend for [ophyd](https://github.com/bluesky/ophyd).

Replaces pyepics (`Python → ctypes → libca.so`) with [epics-rs](https://github.com/epics-rs/epics-rs) (`Python → PyO3 → Rust CA client`), releasing the GIL during all network I/O.

## Installation

```bash
pip install ophyd-epicsrs
```

Building from source requires a Rust toolchain (1.85+):

```bash
pip install maturin
maturin develop
```

## Usage

```python
import ophyd
ophyd.set_cl("epicsrs")

# Or via environment variable
# OPHYD_CONTROL_LAYER=epicsrs

# All ophyd devices now use the Rust CA backend
motor = ophyd.EpicsMotor("IOC:m1", name="motor1")
motor.wait_for_connection(timeout=5)
print(motor.read())
```

Or with the convenience function:

```python
from ophyd_epicsrs import use_epicsrs_backend
use_epicsrs_backend()
```

## Architecture

```
ophyd (Python)
  └── _epicsrs_shim.py          ophyd control layer interface
        └── ophyd_epicsrs        this package
              └── _native.so     PyO3 bindings
                    └── epics-rs pure Rust CA/PVA client (no libca.so)
```

### GIL behavior

| Operation | GIL |
|-----------|-----|
| CA get / put | **released** — `py.allow_threads()` → tokio async |
| CA monitor receive | **released** — tokio background task |
| Monitor callback → Python | **held** — `Python::with_gil()` |
| Connection wait | **released** — tokio async |

### Key types

- **`EpicsRsContext`** — Shared tokio runtime + CA client. One per session.
- **`EpicsRsPV`** — PV channel wrapper. Implements `wait_for_connection`, `get_with_metadata`, `put`, `add_monitor_callback`.

## Requirements

- Python >= 3.10
- ophyd >= 1.7
- [epics-rs](https://github.com/epics-rs/epics-rs) (bundled at build time, no runtime dependency)

## Related

- [bluesky-dataforge](https://github.com/physwkim/bluesky-dataforge) — Rust-accelerated document subscriber for bluesky
- [epics-rs](https://github.com/epics-rs/epics-rs) — Pure Rust EPICS implementation

## License

BSD 3-Clause
