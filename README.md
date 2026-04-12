# raknet wrapper
**fixes**
- `pendingManualSends` used string concatenation as a key which could collide on certain payloads, switched to exact byte comparison
- `hookRegistered` was never reset between executions, added `teardown()` to handle that
- `waitfor` was busy-polling with `task.wait()`, now uses a proper coroutine yield + signal wakeup
- `table.remove(t, 1)` on every captured packet was O(n), replaced with a fixed-size ring buffer
- `Disconnect` did a linear scan over the callback list, now O(1) via subscriber ID map

**new**
- `raknet.use(fn)`: middleware pipeline, return nil to drop a packet or a modified copy to transform it
- `raknet.sendmany(packets)`: batch send with per-packet results
- `raknet.Capture:Promise(predicate?)`: promise-style capture for use inside coroutines
- `raknet.setrecentlimit(n)`: configure the ring buffer size at runtime
- `raknet.teardown()`: full cleanup, safe to call multiple times
## added funcs:

- `raknet.sendraw(...)`
- `raknet.sendhex(...)`
- `raknet.sendstring(...)`
- `raknet.sendopcode(...)`
- `raknet.sendmany(...)`
- `raknet.resend(...)`
- `raknet.sendlike(...)`
- `raknet.startcapture()`
- `raknet.stopcapture()`
- `raknet.setfilter(...)`
- `raknet.clearfilter()`
- `raknet.blockopcode(...)`
- `raknet.clearrecent()`
- `raknet.recent(...)`
- `raknet.setrecentlimit(...)`
- `raknet.clonepacket(...)`
- `raknet.matchprefix(...)`
- `raknet.use(...)`
- `raknet.clearmiddleware()`
- `raknet.setmode(...)`
- `raknet.getmode()`
- `raknet.dryrun(...)`
- `raknet.teardown()`
- `raknet.Capture:Connect(...)`
- `raknet.Capture:Once(...)`
- `raknet.Capture:Promise(...)`
- `raknet.Capture:ConnectOpcode(...)`
- `raknet.Capture:ConnectPrefix(...)`
- `raknet.Capture:ConnectMatch(...)`
- `raknet.tohex(...)`
- `raknet.fromhex(...)`
- `raknet.hexdiff(...)`
- `raknet.packettostring(...)`
- `raknet.countopcodes(...)`
- `raknet.waitfor(...)`
- `raknet.captureonce(...)`
- `raknet.stats()`
- `raknet.resetstats()`
- `raknet.getlasterror()`

## API

### Send helpers

`raknet.sendraw(value, priority?, reliability?, orderingChannel?) -> (boolean, string?)`

- `value`: `string | {number}`
- returns:
  - `true` if the packet was accepted
  - `false, "blocked by filter"` if the local filter blocked it
  - `false, "blocked by middleware"` if a middleware dropped it
  - `false, <error>` if the native send failed

`raknet.sendhex(value, priority?, reliability?, orderingChannel?) -> (boolean, string?)`

- `value`: `string` - hex string, spaces optional (`"83 00 01"` or `"830001"`)

`raknet.sendstring(value, priority?, reliability?, orderingChannel?) -> (boolean, string?)`

- `value`: `string` - sent as raw bytes

`raknet.sendopcode(id, payload?, priority?, reliability?, orderingChannel?) -> (boolean, string?)`

- `id`: `number`
- `payload`: `{number}?` - appended after the opcode byte

`raknet.sendmany(packets) -> { { ok: boolean, err: string? } }`

- `packets`: `{ { bytes: {number}, priority?: number, reliability?: number, orderingChannel?: number } }`
- sends each packet in order and returns a result table per entry

`raknet.resend(packet, priority?, reliability?, orderingChannel?) -> (boolean, string?)`

- `packet`: `{ data: {number}, priority?: number, reliability?: number, orderingChannel?: number }`
- resends the packet data, using its stored transport values unless overridden

`raknet.sendlike(packet, newBytes) -> (boolean, string?)`

- `packet`: `{ priority?: number, reliability?: number, orderingChannel?: number }`
- `newBytes`: `{number} | string`
- sends replacement bytes with the same transport values as the source packet

### Capture helpers

`raknet.startcapture() -> ()`

`raknet.stopcapture() -> ()`

`raknet.iscapturing() -> boolean`

`raknet.Capture:Connect(fn) -> { Disconnect: (self) -> () }`

- `fn`: `(packet) -> ()`

`raknet.Capture:Once(fn) -> { Disconnect: (self) -> () }`

`raknet.Capture:Promise(predicate?) -> { andThen, catch, cancel }`

- `predicate`: `((packet) -> boolean)?` - optional filter, resolves on any packet if omitted
- resolves with the first matching packet
- `cancel()` disconnects without firing any callbacks

`raknet.Capture:ConnectOpcode(id, fn) -> { Disconnect: (self) -> () }`

- `id`: `number`
- `fn`: `(packet) -> ()`

`raknet.Capture:ConnectPrefix(prefix, fn) -> { Disconnect: (self) -> () }`

- `prefix`: `{number}`
- `fn`: `(packet) -> ()`

`raknet.Capture:ConnectMatch(predicate, fn) -> { Disconnect: (self) -> () }`

- `predicate`: `(packet) -> boolean`
- `fn`: `(packet) -> ()`

`raknet.waitfor(id?, timeout?) -> packet?`

- `id`: `number?` - opcode to wait for, or any packet if nil
- `timeout`: `number?` - seconds, default 5
- yields the coroutine until a match arrives or the deadline expires

`raknet.captureonce(id?, timeout?) -> packet?`

- same as `waitfor` but temporarily enables capture if it wasn't already on

### Filter helpers

`raknet.setfilter(bytes?) -> ()`

- `bytes`: `{number}?` - prefix to block; passing nil or `{}` clears the filter

`raknet.clearfilter() -> ()`

`raknet.getfilter() -> {number}`

`raknet.blockopcode(id) -> ()`

- `id`: `number`

### Middleware

`raknet.use(fn) -> { Remove: (self) -> () }`

- `fn`: `(packet: PacketSnapshot) -> PacketSnapshot?`
- runs on every outgoing packet before it is sent
- return a (possibly modified) copy to continue, or `nil` to drop the packet
- middlewares run in insertion order
- returns a handle with `:Remove()` to unregister

`raknet.clearmiddleware() -> ()`

### Replay / history helpers

`raknet.clearrecent() -> ()`

`raknet.recent(limit?, source?) -> {packet}`

- `limit`: `number?`
- `source`: `string?` - `"hook"`, `"manual"`, `"manual-dryrun"`, etc.
- returns packets newest-first

`raknet.setrecentlimit(limit) -> ()`

- `limit`: `number` - max entries to keep in the ring buffer, default 200

`raknet.clonepacket(packet) -> packet`

`raknet.matchprefix(bytes, prefix) -> boolean`

- `bytes`: `{number} | string`
- `prefix`: `{number} | string`

### Transport mode

`raknet.setmode(mode) -> ()`

- `mode`: `"live" | "dryrun"`

`raknet.getmode() -> string`

`raknet.dryrun(enabled?) -> ()`

- `enabled`: `boolean?` - pass `false` to switch back to live

### Formatting helpers

`raknet.tohex(bytes, limit?) -> string`

- `bytes`: `{number} | string`
- `limit`: `number?` - truncates with `...` if the payload exceeds this

`raknet.fromhex(value) -> {number}`

- `value`: `string`

`raknet.hexdiff(left, right) -> string`

- `left`: `{number} | string`
- `right`: `{number} | string`

`raknet.packettostring(packet) -> string`

- `packet.id`: `number?`
- `packet.data`: `{number}?`
- `packet.source`: `string?`
- `packet.blocked`: `boolean?`

### Stats helpers

`raknet.stats() -> table`

Returns a snapshot containing:

- `sent: number`
- `blocked: number`
- `captured: number`
- `hookPackets: number`
- `manualPackets: number`
- `liveSends: number`
- `dryrunSends: number`
- `sendErrors: number`
- `lastSendError: string?`
- `mode: string`
- `byOpcode: { [number]: { sent: number, blocked: number, captured: number } }`

`raknet.resetstats() -> ()`

`raknet.getlasterror() -> string?`

`raknet.countopcodes(seconds?, source?) -> { [number]: number }`

- `seconds`: `number?` - capture window, default 3
- `source`: `string?`

### Teardown

`raknet.teardown() -> ()`

Fully unregisters the hook, clears all subscribers, pending sends, middleware,
filter, ring buffer, and stats. Safe to call multiple times. After this the
wrapper can be re-executed from a clean slate.

## Basic examples

Send a raw byte table:

```luau
raknet.sendraw({ 0x83, 0x00, 0x01 })
```

Send a hex string:

```luau
raknet.sendhex("83 00 01")
```

Send a packet by opcode:

```luau
raknet.sendopcode(0x83, { 0x00, 0x01 })
```

Send multiple packets at once:

```luau
local results = raknet.sendmany({
    { bytes = { 0x83, 0x00, 0x01 } },
    { bytes = { 0x84, 0xFF }, priority = 1 },
})
for i, r in ipairs(results) do
    if not r.ok then
        print("packet", i, "failed:", r.err)
    end
end
```

Re-send a captured packet:

```luau
local packet = raknet.captureonce(0x83, 5)
if packet then
    raknet.resend(packet)
end
```

Send new bytes with the same transport values as a real packet:

```luau
local packet = raknet.captureonce(0x83, 5)
if packet then
    raknet.sendlike(packet, { 0x83, 0x99, 0x01 })
end
```

## Capture

Start capture and listen for all packets:

```luau
raknet.startcapture()

local conn = raknet.Capture:Connect(function(packet)
    print(raknet.packettostring(packet))
end)
```

Listen for a specific opcode:

```luau
local conn = raknet.Capture:ConnectOpcode(0x83, function(packet)
    print("got 0x83:", raknet.tohex(packet.data))
end)
```

One-shot listener:

```luau
raknet.Capture:Once(function(packet)
    print("first packet:", packet.id)
end)
```

Promise-style (useful inside coroutines):

```luau
raknet.Capture:Promise(function(packet)
    return packet.id == 0x83
end):andThen(function(packet)
    print("resolved:", raknet.packettostring(packet))
end)
```

Match by prefix:

```luau
local conn = raknet.Capture:ConnectPrefix({ 0x83, 0x07 }, function(packet)
    print("prefix match:", raknet.packettostring(packet))
end)
```

Match with a custom predicate:

```luau
local conn = raknet.Capture:ConnectMatch(function(packet)
    return packet.id == 0x83 and packet.data[2] == 0x07
end, function(packet)
    print("match:", raknet.packettostring(packet))
end)
```

Stop capture:

```luau
raknet.stopcapture()
conn:Disconnect()
```

## Filters

Block by prefix:

```luau
raknet.setfilter({ 0x83, 0x00 })
```

Block by opcode:

```luau
raknet.blockopcode(0x83)
```

Clear filter:

```luau
raknet.clearfilter()
```

important: filtering is prefix-based.  
`{ 0x83 }` blocks any packet starting with `0x83`.  
`{ 0x83, 0x00 }` only blocks packets whose first two bytes are `83 00`.

## Middleware

Log and optionally mutate every outgoing packet:

```luau
local handle = raknet.use(function(packet)
    print("outgoing:", raknet.packettostring(packet))
    return packet -- pass through unchanged
end)
```

Drop a packet by returning nil:

```luau
raknet.use(function(packet)
    if packet.id == 0x83 then
        return nil -- silently dropped
    end
    return packet
end)
```

Mutate the payload before send:

```luau
raknet.use(function(packet)
    local p = raknet.clonepacket(packet)
    p.data[2] = 0xFF
    p.size    = #p.data
    return p
end)
```

Unregister a specific middleware:

```luau
handle:Remove()
```

Clear all middleware:

```luau
raknet.clearmiddleware()
```

## Dry run mode

Test send logic without actually transmitting:

```luau
raknet.dryrun(true)

raknet.sendopcode(0x83, { 0x00, 0x01 }) -- no packet leaves the client
print(raknet.stats().dryrunSends)        -- 1

raknet.dryrun(false) -- back to live
```

## Return values

Most send helpers return two values:

```luau
local ok, err = raknet.sendopcode(0x83, { 0x00, 0x01 })
```

Possible outcomes:

- `true` - packet accepted by the wrapper
- `false, "blocked by filter"` - the local filter matched
- `false, "blocked by middleware"` - a middleware returned nil
- `false, <executor error>` - native live send failed

## Formatting helpers

Bytes to hex:

```luau
print(raknet.tohex({ 0x83, 0x00, 0x01 }))
-- 83 00 01
```

Hex to bytes:

```luau
local bytes = raknet.fromhex("83 00 01")
```

Hex diff:

```luau
print(raknet.hexdiff({ 0x83, 0x00, 0x01 }, { 0x83, 0x07, 0x01 }))
-- 01: 83
-- 02: 00 -> 07
-- 03: 01
```

Packet summary:

```luau
print(raknet.packettostring({
    id      = 0x83,
    data    = { 0x83, 0x00, 0x01 },
    source  = "manual",
    blocked = false,
}))
```

## Stats

Get counters:

```luau
local s = raknet.stats()
print(s.sent, s.captured, s.blocked)
```

Per-opcode breakdown:

```luau
local s = raknet.stats()
for id, bucket in pairs(s.byOpcode) do
    print(string.format("0x%02X  sent=%d blocked=%d captured=%d", id, bucket.sent, bucket.blocked, bucket.captured))
end
```

Reset counters:

```luau
raknet.resetstats()
```

Count opcodes over a capture window:

```luau
local counts = raknet.countopcodes(3, "hook")
for id, n in pairs(counts) do
    print(string.format("0x%02X x%d", id, n))
end
```

Read recent captured packets:

```luau
local packets = raknet.recent(10, "hook")
for _, p in ipairs(packets) do
    print(raknet.packettostring(p))
end
```

Last native send error:

```luau
print(raknet.getlasterror())
```

## Teardown

```luau
raknet.teardown()
-- wrapper is fully reset; safe to re-execute the script
```

## Note

- markdown template made by @dodger1911#0
