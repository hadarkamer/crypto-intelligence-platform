# Command Specification V1

| Command | Scans? | Saves DB? | Starts Watch? |
|---|---:|---:|---:|
| `/collect` | Yes, once | Yes | No |
| `/alerts` | Yes, once | No | No |
| `/coin SYMBOL` | No | No | No |
| `/watch_on` | Repeated | No | Yes |
| `/watch_status` | No | No | No |
| `/watch_stop` | No | No | Stops it |

A score is never calculated from a partial timeframe set.
The Watch task may only be created inside `watch_on()`.
