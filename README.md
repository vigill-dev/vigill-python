# Vigil — Python SDK (`vigill-sdk`)

Plain-English production error monitoring for Python apps. Standard library only, no dependencies. Python 3.8+.

Vigil turns crashes into **what broke / who's affected / is it costing money / how urgent**, plus a copy-paste **Fix Prompt** for your AI coding tool.

> **Install name is `vigill-sdk`; the import is `vigil`.**

```bash
pip install vigill-sdk
```

```python
import vigil

vigil.init(key="vg_pub_your_public_key")

# Uncaught exceptions are captured automatically (via sys.excepthook).
# For handled ones:
try:
    charge_card(order)
except Exception:
    vigil.capture_exception()
    raise

vigil.capture_message("worker started", level="info")
```

> Your `key` is public and write-only — it can only send events. You can also supply it via the `VIGIL_KEY` env var (an explicit `key` wins).

## API

- `init(key=None, endpoint=None, environment=None, release=None, tags=None, debug=False)`
- `capture_exception(exc=None, tags=None)` — call inside an `except` block
- `capture_message(message, level="info", tags=None)`
- `flush()` / `close()`

### Endpoint

You never set `endpoint`. It defaults to the hosted `https://vigill.dev/api/ingest`. Resolution order if you do: explicit `endpoint` → `VIGIL_ENDPOINT` env var → hosted default.

## How it behaves

Events are batched and sent on a daemon thread and on interpreter exit. The uncaught hook chains to whatever `sys.excepthook` was already installed, so a crash behaves exactly as it would without Vigil. With no key from either source the SDK stays disabled rather than raising — it must never break the host process.

## License

MIT © Rajeev Chourey
