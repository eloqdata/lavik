# Active expiration tuning

Each worker starts an expiration/index-maintenance coroutine after storage
recovery. It remains present even without TTL keys. Runtime settings are shared
by all workers and can be inspected with:

```text
CONFIG GET active-expiration-*
```

| Parameter | Default | Meaning |
|---|---:|---|
| `active-expiration-interval-ms` | 10 | Sleep in milliseconds before each cycle |
| `active-expiration-map-steps-per-cycle` | 256 | Maximum expiration scan steps per cycle; a step skips one partition/DB map without TTL keys or advances its index scan cursor |
| `active-expiration-deletes-per-cycle` | 64 | Maximum queued deletion candidates processed per cycle, including stale candidates and failed deletion attempts |
| `active-expiration-index-maintenance-steps-per-cycle` | 256 | Maximum index-maintenance steps per cycle, including on replicas and maps without TTL keys |

Each parameter accepts an integer from 1 through 4,294,967,295. Zero, negative,
non-integer, and overflowing values are rejected without changing the setting.

```text
CONFIG SET active-expiration-interval-ms 50
CONFIG SET active-expiration-map-steps-per-cycle 512
CONFIG SET active-expiration-deletes-per-cycle 128
CONFIG SET active-expiration-index-maintenance-steps-per-cycle 128
```

Workers sample work budgets after waking and keep those values for the entire
cycle. An interval update changes the next sleep; an existing sleep finishes
with its original duration. The interval is a delay between work batches, not
a bound on how quickly every expired key is collected. Longer intervals or
smaller budgets reduce background work per unit time but delay cleanup. Larger
budgets increase each cycle's work; index maintenance runs without yielding
inside its batch. TTL checks on reads still return expired keys as absent.

These parameters only control pacing. Existing expiration authority and
pause/drain rules still apply. They do not stop the coroutine when TTL counts
reach zero, and they do not configure Tomb Raider's separate tombstone sweep.

Changes last for the current process. These parameters have no startup-file or
CLI options, and `CONFIG REWRITE` does not persist them. To restore defaults
without restarting, set the four values to 10, 256, 64, and 256 respectively.
