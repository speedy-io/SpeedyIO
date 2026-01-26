# SpeedyIO Config File – Read Me First

This explains exactly how to write the `speedyio.conf` that your loader understands. It covers syntax, quoting, env-var expansion, lists, paths, addresses, URLs, validation rules, and common errors—with copy‑pasteable examples.

---

## Quick basics

* **Format:** `key = value`
* **Whitespace:** ignored around keys, `=`, and values.
* **Comments:** `#` or `;` to end of line (unless inside quotes).
* **Unknown keys:** rejected (loader is schema‑driven). Either add to schema or remove from file.
* **Required keys:** must be present; the loader errors if missing.

Example:

```conf
queue_depth = 128
data_dir    = "$HOME/data"   # expands env and ~
```

---

## Types you can use

Your schema defines each key’s type and constraints. The loader knows these types:

* `OPT_STR` – string
* `OPT_INT` – integer (with min/max)
* `OPT_BOOL` – boolean (`1/0`, `true/false`, `yes/no`, `on/off`, case‑insensitive)
* `OPT_PATH` – path string; expands `~` and `$VARS`; enforces existence flags
* `OPT_STR_LIST` – list of strings
* `OPT_INT_LIST` – list of integers
* `OPT_PATH_LIST` – list of paths (each expanded & validated)
* `OPT_ADDR` – **host\:port** (IPv4/hostname) or **\[IPv6]\:port**
* `OPT_URL` – `http://` or `https://` URL with optional path/query/fragment

> Lists accept **comma‑separated** values **and/or** **repeated keys** (both append).

---

## Quoting & expansion rules (critical)

| How you write it          | What happens                                                                     |
| ------------------------- | -------------------------------------------------------------------------------- |
| `value` (unquoted)        | Shell‑like expansion with `wordexp`: `~` and `$VARS` expand; spaces split words. |
| `"value"` (double‑quoted) | Expansion **happens**, but is kept as **one token** (spaces preserved).          |
| `'value'` (single‑quoted) | **Literal**. No expansion. `$`, `~`, `*` are just characters.                    |

**Paths (`OPT_PATH`/`OPT_PATH_LIST`):**

* Unquoted is allowed, but if the path contains spaces you’ll fight tokenization.
* **Best practice:** wrap in **double quotes** when you want expansion *and* spaces kept:
  `data_dir = "$HOME/my dir"`

**Integers/booleans:** after expansion they must be a **single token**. Don’t leave spaces.

Examples:

```conf
# Strings
title   = "hello world"      # becomes "hello world"
raw     = '$HOME/${USER}'    # literal: $HOME/${USER}

# Int/bool with env
QD      = $QD                # expands to number
enabled = yes
enabled = "no"               # also fine

# Paths
data_dir = "$HOME/adf ad"    # expands; spaces kept
out_dir  = '$HOME/adf ad'    # literal string; no expansion
logs     = $HOME/logs        # ok if no spaces
```

---

## Lists (arrays)

Two ways to write lists. You can mix them.

### 1) Comma‑separated list

* Quotes control expansion per **item**.
* Comments allowed outside quotes.

```conf
include = "/etc/my app", "$HOME/with space", '/literal $HOME/noexpand'
devices = sda, sdb, "nvme weird"
queue_depths = 32, 64, $QD
```

### 2) Repeated key appends

* Each occurrence appends to the same list.

```conf
include = "$HOME/a"
include = "$HOME/b"
queue_depths = 256
```

### Capacity

Your app allocates fixed list capacities (e.g., `MAX_INCLUDES`). Exceeding them is an error.

---

## Paths & validation flags

For `OPT_PATH` and `OPT_PATH_LIST`, the schema can enforce existence:

* `OPTF_PATH_MUST_EXIST` – each expanded path must already exist.
* `OPTF_PATH_MAYNOT_EXIST` – each expanded path **must not** already exist (e.g., create later).
* You can’t set both on the same key.

**Expansion behavior for paths:**

* Single‑quoted → **literal** (no expansion).
* Double‑quoted or unquoted → expansion happens; the loader re‑quotes internally so a spaced path remains **one** token.
* Globs/expansions that produce **multiple** tokens are rejected for path types.

Examples:

```conf
data_dir   = "$HOME/data"     # must exist if schema says so
output_dir = "$HOME/new out"  # may not exist if schema says so
include    = "/etc/my app", "$HOME/with space"
```

---

## Addresses (`OPT_ADDR`)

Syntax:

* IPv4 or hostname: `host:port`
  `server = example.com:443`
  `server = 192.168.1.10:8080`
* IPv6 **must** be bracketed: `[2001:db8::1]:8443`
* `port` must be `1..65535`

Expansion works unless you single‑quote:

```conf
server = "$HOSTNAME":$PORT
# or
server = [::1]:443
```

**Invalid examples (rejected):**

* `2001:db8::1:443` (missing brackets)
* `host:` (missing port)
* `:443` (missing host)
* `host:70000` (port out of range)

---

## URLs (`OPT_URL`)

* Only `http://` or `https://`
* Host can be hostname, IPv4, or `[IPv6]`
* Optional `:port` and any path/query/fragment
* No spaces after expansion

Examples:

```conf
api_base = "https://example.com/api/v1?token=$API_TOKEN"
cdn      = http://cdn.example.com/assets/index.html
ipv6url  = "https://[2001:db8::1]:8443/path?q=1#frag"
```

**Rejected:** `ftp://…`, missing scheme, spaces in URL after expansion.

---

## Booleans (`OPT_BOOL`)

Accepted values (case‑insensitive):
`1/0`, `true/false`, `yes/no`, `on/off`.

```conf
enable_eviction = true
verbose         = OFF
```

---

## Integers (`OPT_INT`, `OPT_INT_LIST`)

* Parsed with `strtoll` base 0 (so `0x10` works).
* Must be within schema min/max (inclusive).
* After expansion, must be a single token.

```conf
queue_depth  = 128
queue_depths = 32, 64, $QD, "256"
```

---

## Comments

* Start with `#` or `;` and run to end of line.
* Inside quotes, they are literal.
* Inline comments are allowed after values:

```conf
data_dir = "$HOME/data"   # this is fine
```

---

## Encoding & gotchas

* The loader **normalizes** common Unicode junk: smart quotes to ASCII, NBSP to space, and strips BOM/zero‑width chars. That saves you from copy/paste surprises.
* **Still your responsibility** to keep quotes matched.

**Common mistakes that trigger errors:**

* Unmatched quotes:

  ```conf
  data_dir = "$HOME/my dir     # missing closing "
  ```
* Wrong IPv6 form:

  ```conf
  server = 2001:db8::1:443     # must be [2001:db8::1]:443
  ```
* Path existence flags violated:

  ```conf
  output_dir = "/tmp/existing" # schema says MAYNOT_EXIST → error
  ```
* Multi‑word expansion where only one token is allowed (e.g., PATH/URL/INT):

  ```conf
  data_dir = $MULTI_WORD_VAR   # expansion splits → error
  ```

---

## Error messages (what they mean)

* `unknown key 'foo'`
  The key isn’t in the schema. Add it to the code or remove it from the file.

* `unterminated quoted value for key 'X'`
  You opened `'` or `"` and never closed it on that line.

* `failed to expand path for key 'X': 'value' (syntax error …)`
  `wordexp()` saw bad shell syntax after our normalization (usually unmatched quotes in the value, or a var expanding into junk). Fix the value or the env var.

* `path for key 'X' does not exist: '…'`
  You set `OPTF_PATH_MUST_EXIST` and the path isn’t there.

* `path for key 'X' already exists: '…'`
  You set `OPTF_PATH_MAYNOT_EXIST` but the path exists.

* `invalid address for key 'server': '…'`
  Doesn’t match `host:port` or `[IPv6]:port`, or port is out of range.

* `invalid URL for key 'api_base': '…'`
  Not `http(s)://`, has spaces, or host/port invalid.

* `value too long for key 'X'`
  You overflowed the destination buffer (increase buffer size in your struct or shorten the string).

* `too many entries for 'X' (cap=N)`
  You exceeded the list capacity (`MAX_*` in your app).

---

## Realistic example config

```conf
# ---- Required basics
device      = nvme0n1
data_dir    = "$HOME/speedyio/data"      # must exist (schema flag)
queue_depth = 128
enable_eviction = true

# ---- Network
server   = [2001:db8::1]:8443            # IPv6 server with port
api_base = "https://api.example.com/v1?token=$API_TOKEN"

# ---- Lists (either style works; you can mix)
include = "/etc/speedyio", "$HOME/speedyio/include"
include = "$HOME/speedyio/extra"

devices = sda, sdb, "nvme special"
queue_depths = 32, 64
queue_depths = 128, $QD

# ---- Literal (no expansion)
raw_label = '$HOME is not expanded here'
```

---

## Troubleshooting checklist

1. **Show line numbers** to catch the bad line fast:

   ```bash
   nl -ba speedyio.conf | sed -n '1,200p'
   ```
2. **Inspect raw bytes** if something makes no sense (hidden Unicode):

   ```bash
   sed -n '12p' speedyio.conf | xxd -g1
   ```
3. **Verify env vars** actually exist in your shell:

   ```bash
   echo "$API_TOKEN" "$QD" "$HOME"
   ```
4. **Quote paths with spaces** using **double quotes** unless you want literal:

   ```conf
   include = "$HOME/with space"
   ```
5. **IPv6 needs brackets** around the host:

   ```conf
   server = [::1]:443
   ```

---

## What’s enforced by code vs. by schema

* **Code:** syntax, quoting, env/tilde expansion, list splitting, address/URL shape, path single‑token guarantee, and existence checks.
* **Schema (your `option_spec_t` array):** which keys exist, their **types**, min/max for ints, **flags** (required, path must/must‑not exist), and list capacities (via your struct sizes).

If you want a new key, add it to the schema with the right type/flags and a destination field in your config struct. That’s it.

---
