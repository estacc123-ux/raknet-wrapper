local nativeRakNet = getgenv().raknet
assert(type(nativeRakNet) == "table",                     "global raknet table is missing")
assert(type(nativeRakNet.send) == "function",             "raknet.send is missing")
assert(type(nativeRakNet.add_send_hook) == "function",    "raknet.add_send_hook is missing")
assert(type(nativeRakNet.remove_send_hook) == "function", "raknet.remove_send_hook is missing")

setreadonly(nativeRakNet, false)

local nativeSend           = nativeRakNet.send
local nativeAddSendHook    = nativeRakNet.add_send_hook
local nativeRemoveSendHook = nativeRakNet.remove_send_hook

local previousHook = rawget(nativeRakNet, "__wrapperHook")
if type(previousHook) == "function" then
    pcall(nativeRemoveSendHook, previousHook)
    rawset(nativeRakNet, "__wrapperHook", nil)
end

-- types

type PacketData     = { number }
type PacketSnapshot = {
    id:              number,
    data:            PacketData,
    size:            number,
    priority:        number,
    reliability:     number,
    orderingChannel: number,
    source:          string,
    blocked:         boolean,
}
type CaptureCallback = (packet: PacketSnapshot) -> ()
type MiddlewareFn    = (packet: PacketSnapshot) -> PacketSnapshot?

-- state

local filterBytes:     PacketData        = {}
local capturing        = false
local hookRegistered   = false
local middlewareChain: { MiddlewareFn }  = {}
local captureSubscribers: { [number]: CaptureCallback } = {}
local nextSubscriberId = 0

type PendingEntry = {
    bytes:           PacketData,
    priority:        number,
    reliability:     number,
    orderingChannel: number,
    count:           number,
}
local pendingManualSends: { PendingEntry } = {}

local transportMode: string =
    rawget(nativeRakNet, "__transportMode") == "dryrun" and "dryrun" or "live"

local RECENT_LIMIT                   = 200
local ringBuffer: { PacketSnapshot } = table.create(RECENT_LIMIT)
local ringHead                       = 0
local ringCount                      = 0

local stats = {
    sent          = 0,
    blocked       = 0,
    captured      = 0,
    hookPackets   = 0,
    manualPackets = 0,
    liveSends     = 0,
    dryrunSends   = 0,
    sendErrors    = 0,
    lastSendError = nil :: string?,
    byOpcode      = {} :: { [number]: { sent: number, blocked: number, captured: number } },
}

-- helpers

local function copyBytes(bytes: PacketData): PacketData
    local n   = #bytes
    local out = table.create(n)
    for i = 1, n do
        out[i] = bytes[i]
    end
    return out
end

local function clonePacketSnapshot(p: PacketSnapshot): PacketSnapshot
    local data = copyBytes(p.data or {})
    return {
        id              = p.id              or data[1] or 0,
        data            = data,
        size            = p.size            or #data,
        priority        = p.priority        or 0,
        reliability     = p.reliability     or 0,
        orderingChannel = p.orderingChannel or 0,
        source          = p.source          or "unknown",
        blocked         = p.blocked == true,
    }
end

local function compactHex(value: string): string
    return value:gsub("%s+", "")
end

local function isHexString(value: string): boolean
    local compact = compactHex(value)
    return compact ~= "" and compact:match("^[%x]+$") ~= nil and (#compact % 2 == 0)
end

local function ensureOpcodeStat(id: number)
    local bucket = stats.byOpcode[id]
    if not bucket then
        bucket = { sent = 0, blocked = 0, captured = 0 }
        stats.byOpcode[id] = bucket
    end
    return bucket
end

local function pendingBytesEqual(a: PacketData, b: PacketData): boolean
    if #a ~= #b then return false end
    for i = 1, #a do
        if a[i] ~= b[i] then return false end
    end
    return true
end

local function findPendingEntry(
    bytes:           PacketData,
    priority:        number,
    reliability:     number,
    orderingChannel: number
): (number?, PendingEntry?)
    for i = 1, #pendingManualSends do
        local e = pendingManualSends[i]
        if  e.priority        == priority
        and e.reliability     == reliability
        and e.orderingChannel == orderingChannel
        and pendingBytesEqual(e.bytes, bytes)
        then
            return i, e
        end
    end
    return nil, nil
end

local function releasePending(
    bytes:           PacketData,
    priority:        number,
    reliability:     number,
    orderingChannel: number
)
    local idx, entry = findPendingEntry(bytes, priority, reliability, orderingChannel)
    if not idx then return end
    if entry.count <= 1 then
        table.remove(pendingManualSends, idx)
    else
        entry.count -= 1
    end
end

local function matchesFilter(bytes: PacketData): boolean
    if #filterBytes == 0 then return false end
    for i = 1, #filterBytes do
        if bytes[i] ~= filterBytes[i] then return false end
    end
    return true
end

local function matchesPrefix(bytes: PacketData, prefix: PacketData): boolean
    if #prefix == 0     then return true  end
    if #bytes  < #prefix then return false end
    for i = 1, #prefix do
        if bytes[i] ~= prefix[i] then return false end
    end
    return true
end

local function makePacketSnapshot(
    bytes:           PacketData,
    priority:        number,
    reliability:     number,
    orderingChannel: number,
    source:          string,
    blocked:         boolean
): PacketSnapshot
    return {
        id              = bytes[1] or 0,
        data            = copyBytes(bytes),
        size            = #bytes,
        priority        = priority,
        reliability     = reliability,
        orderingChannel = orderingChannel,
        source          = source,
        blocked         = blocked,
    }
end

local function ringPush(packet: PacketSnapshot)
    local slot       = (ringHead % RECENT_LIMIT) + 1
    ringBuffer[slot] = packet
    ringHead         = ringHead + 1
    if ringCount < RECENT_LIMIT then
        ringCount += 1
    end
end

-- capture internals

local function recordCapture(packet: PacketSnapshot)
    stats.captured += 1
    ensureOpcodeStat(packet.id).captured += 1
    ringPush(clonePacketSnapshot(packet))
end

local function fireCapture(packet: PacketSnapshot)
    recordCapture(packet)
    local ids = {}
    for id in pairs(captureSubscribers) do
        ids[#ids + 1] = id
    end
    for i = 1, #ids do
        local fn = captureSubscribers[ids[i]]
        if fn then
            task.spawn(fn, packet)
        end
    end
end

-- middleware pipeline

local function applyMiddleware(snapshot: PacketSnapshot): PacketSnapshot?
    local current: PacketSnapshot? = snapshot
    for i = 1, #middlewareChain do
        if current == nil then return nil end
        current = middlewareChain[i](current)
    end
    return current
end

-- Capture object

local Capture = {}
Capture.__index = Capture

function Capture:Connect(fn: CaptureCallback)
    assert(type(fn) == "function", "Capture:Connect expects a function")

    nextSubscriberId += 1
    local id = nextSubscriberId
    captureSubscribers[id] = fn

    local disconnected = false
    return {
        Disconnect = function(_self)
            if disconnected then return end
            disconnected = true
            captureSubscribers[id] = nil
        end,
    }
end

function Capture:Once(fn: CaptureCallback)
    assert(type(fn) == "function", "Capture:Once expects a function")
    local conn
    conn = self:Connect(function(packet)
        conn:Disconnect()
        fn(packet)
    end)
    return conn
end

function Capture:Promise(predicate: ((PacketSnapshot) -> boolean)?)
    local resolvers: { (PacketSnapshot) -> () } = {}
    local rejecters: { (string) -> () }         = {}
    local settled = false

    local conn
    conn = self:Connect(function(packet)
        if settled then return end
        if predicate and not predicate(packet) then return end
        settled = true
        conn:Disconnect()
        for _, r in ipairs(resolvers) do
            task.spawn(r, packet)
        end
    end)

    return {
        andThen = function(_, fn: (PacketSnapshot) -> ())
            if settled then return end
            resolvers[#resolvers + 1] = fn
        end,
        catch = function(_, fn: (string) -> ())
            rejecters[#rejecters + 1] = fn
        end,
        cancel = function(_)
            if settled then return end
            settled = true
            conn:Disconnect()
        end,
    }
end

function Capture:ConnectOpcode(id: number, fn: CaptureCallback)
    assert(type(id) == "number",   "Capture:ConnectOpcode expects a numeric id")
    assert(type(fn) == "function", "Capture:ConnectOpcode expects a function")
    return self:Connect(function(packet)
        if packet.id == id then fn(packet) end
    end)
end

function Capture:ConnectMatch(predicate, fn: CaptureCallback)
    assert(type(predicate) == "function", "Capture:ConnectMatch expects a predicate function")
    assert(type(fn) == "function",        "Capture:ConnectMatch expects a callback function")
    return self:Connect(function(packet)
        if predicate(packet) then fn(packet) end
    end)
end

function Capture:ConnectPrefix(prefix: PacketData, fn: CaptureCallback)
    assert(type(prefix) == "table", "Capture:ConnectPrefix expects a byte table prefix")
    assert(type(fn) == "function",  "Capture:ConnectPrefix expects a callback function")
    local expected = copyBytes(prefix)
    return self:Connect(function(packet)
        if matchesPrefix(packet.data, expected) then fn(packet) end
    end)
end

-- hook registration

local function ensureHook()
    if hookRegistered then return end

    local function sendHook(packet)
        stats.hookPackets += 1

        local bytes = copyBytes(packet.AsArray)

        local _, pending = findPendingEntry(bytes, packet.Priority, packet.Reliability, packet.OrderingChannel)
        if pending then
            releasePending(bytes, packet.Priority, packet.Reliability, packet.OrderingChannel)
            return
        end

        if matchesFilter(bytes) then
            stats.blocked += 1
            ensureOpcodeStat(bytes[1] or 0).blocked += 1
            packet:Block()
            return
        end

        if capturing then
            fireCapture(makePacketSnapshot(
                bytes, packet.Priority, packet.Reliability, packet.OrderingChannel, "hook", false
            ))
        end
    end

    nativeAddSendHook(sendHook)
    rawset(nativeRakNet, "__wrapperHook", sendHook)
    hookRegistered = true
end

-- hex / byte normalisation

local function normalizeBytes(value: string | PacketData): PacketData
    if type(value) == "table" then
        return copyBytes(value)
    end
    assert(type(value) == "string", "expected string or byte table")
    if isHexString(value) then
        local compact = compactHex(value)
        local bytes   = table.create(#compact // 2)
        for i = 1, #compact, 2 do
            bytes[#bytes + 1] = tonumber(compact:sub(i, i + 1), 16)
        end
        return bytes
    end
    local bytes = table.create(#value)
    for i = 1, #value do bytes[i] = string.byte(value, i) end
    return bytes
end

-- core send path

local function sendBytes(
    bytes:           PacketData,
    priority:        number,
    reliability:     number,
    orderingChannel: number,
    nativeValue:     (PacketData | string)?
): (boolean, string?)
    local id = bytes[1] or 0

    if matchesFilter(bytes) then
        stats.blocked += 1
        ensureOpcodeStat(id).blocked += 1
        return false, "blocked by filter"
    end

    local snapshot = makePacketSnapshot(
        bytes, priority, reliability, orderingChannel,
        transportMode == "dryrun" and "manual-dryrun" or "manual", false
    )
    local transformed = applyMiddleware(snapshot)
    if transformed == nil then
        stats.blocked += 1
        ensureOpcodeStat(id).blocked += 1
        return false, "blocked by middleware"
    end

    bytes           = transformed.data
    priority        = transformed.priority
    reliability     = transformed.reliability
    orderingChannel = transformed.orderingChannel

    stats.sent          += 1
    stats.manualPackets += 1
    ensureOpcodeStat(bytes[1] or 0).sent += 1
    stats.lastSendError  = nil

    if capturing then
        fireCapture(makePacketSnapshot(
            bytes, priority, reliability, orderingChannel,
            transportMode == "dryrun" and "manual-dryrun" or "manual", false
        ))
    end

    if transportMode == "dryrun" then
        stats.dryrunSends += 1
        return true
    end

    local idx, entry = findPendingEntry(bytes, priority, reliability, orderingChannel)
    if idx then
        entry.count += 1
    else
        pendingManualSends[#pendingManualSends + 1] = {
            bytes           = copyBytes(bytes),
            priority        = priority,
            reliability     = reliability,
            orderingChannel = orderingChannel,
            count           = 1,
        }
    end

    stats.liveSends += 1
    local payload = nativeValue ~= nil and nativeValue or copyBytes(bytes)
    local ok, err = pcall(nativeSend, payload, priority, reliability, orderingChannel)
    if not ok then
        releasePending(bytes, priority, reliability, orderingChannel)
        stats.sendErrors    += 1
        stats.lastSendError  = tostring(err)
        return false, tostring(err)
    end

    return true
end

-- public API

local raknet = nativeRakNet

-- send helpers

function raknet.sendraw(value: string | PacketData, priority: number?, reliability: number?, orderingChannel: number?): (boolean, string?)
    local bytes       = normalizeBytes(value)
    local nativeValue = (type(value) == "string" and not isHexString(value)) and value or copyBytes(bytes)
    return sendBytes(bytes, priority or 0, reliability or 0, orderingChannel or 0, nativeValue)
end

function raknet.sendhex(value: string, priority: number?, reliability: number?, orderingChannel: number?): (boolean, string?)
    return raknet.sendraw(raknet.fromhex(value), priority, reliability, orderingChannel)
end

function raknet.sendstring(value: string, priority: number?, reliability: number?, orderingChannel: number?): (boolean, string?)
    assert(type(value) == "string", "sendstring expects a string")
    return sendBytes(normalizeBytes(value), priority or 0, reliability or 0, orderingChannel or 0, value)
end

function raknet.sendopcode(id: number, payload: PacketData?, priority: number?, reliability: number?, orderingChannel: number?): (boolean, string?)
    assert(type(id) == "number", "sendopcode expects a numeric id")
    local bytes: PacketData = { id }
    for _, b in ipairs(payload or {}) do bytes[#bytes + 1] = b end
    return sendBytes(bytes, priority or 0, reliability or 0, orderingChannel or 0)
end

function raknet.sendmany(packets: { { bytes: PacketData, priority: number?, reliability: number?, orderingChannel: number? } }): { { ok: boolean, err: string? } }
    assert(type(packets) == "table", "sendmany expects a table of packet specs")
    local results = table.create(#packets)
    for i, spec in ipairs(packets) do
        assert(type(spec.bytes) == "table", "sendmany: packet #" .. i .. " missing .bytes")
        local ok, err = sendBytes(spec.bytes, spec.priority or 0, spec.reliability or 0, spec.orderingChannel or 0)
        results[i] = { ok = ok, err = err }
    end
    return results
end

-- packet helpers

function raknet.clonepacket(packet): PacketSnapshot
    assert(type(packet) == "table", "clonepacket expects a packet table")
    return clonePacketSnapshot(packet)
end

function raknet.matchprefix(bytes: PacketData | string, prefix: PacketData | string): boolean
    local b = type(bytes)  == "string" and normalizeBytes(bytes)  or copyBytes(bytes)
    local p = type(prefix) == "string" and normalizeBytes(prefix) or copyBytes(prefix)
    return matchesPrefix(b, p)
end

function raknet.resend(packet, priority: number?, reliability: number?, orderingChannel: number?): (boolean, string?)
    assert(type(packet) == "table", "resend expects a packet table")
    return raknet.sendraw(
        copyBytes(packet.data or {}),
        priority        ~= nil and priority        or packet.priority        or 0,
        reliability     ~= nil and reliability     or packet.reliability     or 0,
        orderingChannel ~= nil and orderingChannel or packet.orderingChannel or 0
    )
end

function raknet.sendlike(packet, newBytes: PacketData | string): (boolean, string?)
    assert(type(packet) == "table", "sendlike expects a packet table")
    return raknet.sendraw(newBytes, packet.priority or 0, packet.reliability or 0, packet.orderingChannel or 0)
end

-- transport mode

function raknet.setmode(mode: string)
    assert(mode == "live" or mode == "dryrun", "setmode expects 'live' or 'dryrun'")
    transportMode = mode
    rawset(nativeRakNet, "__transportMode", mode)
end

function raknet.getmode(): string
    return transportMode
end

function raknet.dryrun(enabled: boolean?)
    raknet.setmode(enabled == false and "live" or "dryrun")
end

-- capture control

function raknet.startcapture()
    ensureHook()
    capturing = true
end

function raknet.stopcapture()
    capturing = false
end

function raknet.iscapturing(): boolean
    return capturing
end

-- filter

function raknet.setfilter(bytes: PacketData?)
    filterBytes = bytes and copyBytes(bytes) or {}
    ensureHook()
end

function raknet.clearfilter()
    filterBytes = {}
end

function raknet.getfilter(): PacketData
    return copyBytes(filterBytes)
end

function raknet.blockopcode(id: number)
    assert(type(id) == "number", "blockopcode expects a number")
    raknet.setfilter({ id })
end

-- middleware

function raknet.use(fn: MiddlewareFn)
    assert(type(fn) == "function", "use expects a function")
    middlewareChain[#middlewareChain + 1] = fn
    local removed = false
    return {
        Remove = function(_self)
            if removed then return end
            removed = true
            for i = 1, #middlewareChain do
                if middlewareChain[i] == fn then
                    table.remove(middlewareChain, i)
                    return
                end
            end
        end,
    }
end

function raknet.clearmiddleware()
    middlewareChain = {}
end

-- recent packets

function raknet.clearrecent()
    ringBuffer = table.create(RECENT_LIMIT)
    ringHead   = 0
    ringCount  = 0
end

function raknet.recent(limit: number?, source: string?): { PacketSnapshot }
    local maxCount = math.max(0, limit or ringCount)
    local out      = {}

    for i = 0, ringCount - 1 do
        if #out >= maxCount then break end
        local slot   = ((ringHead - 1 - i) % RECENT_LIMIT) + 1
        local packet = ringBuffer[slot]
        if packet and (source == nil or packet.source == source) then
            out[#out + 1] = clonePacketSnapshot(packet)
        end
    end

    return out
end

function raknet.setrecentlimit(limit: number)
    assert(type(limit) == "number" and limit > 0, "setrecentlimit expects a positive number")
    RECENT_LIMIT = limit
    if ringCount > RECENT_LIMIT then
        ringCount = RECENT_LIMIT
    end
end

-- hex utilities

function raknet.tohex(bytes: PacketData | string, limit: number?): string
    local normalized = type(bytes) == "string" and normalizeBytes(bytes) or copyBytes(bytes)
    local maxCount   = limit or #normalized
    local count      = math.min(#normalized, maxCount)
    local out        = table.create(count)
    for i = 1, count do
        out[i] = string.format("%02X", normalized[i])
    end
    if #normalized > maxCount then
        out[#out + 1] = "..."
    end
    return table.concat(out, " ")
end

function raknet.fromhex(value: string): PacketData
    assert(type(value) == "string", "fromhex expects a string")
    local compact = compactHex(value)
    assert(compact ~= "" and compact:match("^[%x]+$") and #compact % 2 == 0,
        "invalid hex string: " .. value)
    local bytes = table.create(#compact // 2)
    for i = 1, #compact, 2 do
        bytes[#bytes + 1] = tonumber(compact:sub(i, i + 1), 16)
    end
    return bytes
end

function raknet.packettostring(packet: PacketSnapshot): string
    local bytes   = copyBytes(packet.data or {})
    local id      = packet.id     or bytes[1] or 0
    local source  = packet.source  and (" source=" .. tostring(packet.source))  or ""
    local blocked = packet.blocked and " blocked=true" or ""
    return string.format("id=0x%02X len=%d bytes=[%s]%s%s",
        id, #bytes, raknet.tohex(bytes, 48), source, blocked)
end

function raknet.hexdiff(left: PacketData | string, right: PacketData | string): string
    local a      = type(left)  == "string" and normalizeBytes(left)  or copyBytes(left)
    local b      = type(right) == "string" and normalizeBytes(right) or copyBytes(right)
    local maxLen = math.max(#a, #b)
    local lines  = table.create(maxLen)
    for i = 1, maxLen do
        local av, bv = a[i], b[i]
        if av == bv then
            lines[#lines + 1] = string.format("%02d: %s", i,
                av ~= nil and string.format("%02X", av) or "--")
        else
            lines[#lines + 1] = string.format("%02d: %s -> %s", i,
                av ~= nil and string.format("%02X", av) or "--",
                bv ~= nil and string.format("%02X", bv) or "--")
        end
    end
    return table.concat(lines, "\n")
end

-- async helpers

function raknet.waitfor(id: number?, timeout: number?): PacketSnapshot?
    ensureHook()

    local deadline = os.clock() + (timeout or 5)
    local result:  PacketSnapshot? = nil
    local thread   = coroutine.running()

    local conn
    conn = raknet.Capture:Connect(function(packet)
        if id ~= nil and packet.id ~= id then return end
        conn:Disconnect()
        result = packet
        if coroutine.status(thread) == "suspended" then
            task.spawn(thread)
        end
    end)

    while result == nil and os.clock() < deadline do
        local remaining = deadline - os.clock()
        task.delay(math.min(remaining, 0.5), function()
            if coroutine.status(thread) == "suspended" then
                task.spawn(thread)
            end
        end)
        coroutine.yield()
    end

    conn:Disconnect()
    return result
end

function raknet.captureonce(id: number?, timeout: number?): PacketSnapshot?
    local wasCapturing = capturing
    raknet.startcapture()
    local packet = raknet.waitfor(id, timeout)
    if not wasCapturing then
        raknet.stopcapture()
    end
    return packet
end

function raknet.countopcodes(seconds: number?, source: string?): { [number]: number }
    local duration     = math.max(0.05, seconds or 3)
    local wasCapturing = capturing
    local counts       = {}
    local conn

    raknet.startcapture()
    conn = raknet.Capture:Connect(function(packet)
        if source ~= nil and packet.source ~= source then return end
        counts[packet.id] = (counts[packet.id] or 0) + 1
    end)

    task.wait(duration)

    conn:Disconnect()
    if not wasCapturing then
        raknet.stopcapture()
    end
    return counts
end

-- stats

function raknet.resetstats()
    stats.sent          = 0
    stats.blocked       = 0
    stats.captured      = 0
    stats.hookPackets   = 0
    stats.manualPackets = 0
    stats.liveSends     = 0
    stats.dryrunSends   = 0
    stats.sendErrors    = 0
    stats.lastSendError = nil
    stats.byOpcode      = {}
end

function raknet.getlasterror(): string?
    return stats.lastSendError
end

function raknet.stats()
    local opcodeStats = {}
    for id, bucket in pairs(stats.byOpcode) do
        opcodeStats[id] = { sent = bucket.sent, blocked = bucket.blocked, captured = bucket.captured }
    end
    return {
        sent          = stats.sent,
        blocked       = stats.blocked,
        captured      = stats.captured,
        hookPackets   = stats.hookPackets,
        manualPackets = stats.manualPackets,
        liveSends     = stats.liveSends,
        dryrunSends   = stats.dryrunSends,
        sendErrors    = stats.sendErrors,
        lastSendError = stats.lastSendError,
        mode          = transportMode,
        byOpcode      = opcodeStats,
    }
end

-- teardown
function raknet.teardown()
    local hook = rawget(nativeRakNet, "__wrapperHook")
    if type(hook) == "function" then
        pcall(nativeRemoveSendHook, hook)
        rawset(nativeRakNet, "__wrapperHook", nil)
    end

    hookRegistered     = false
    capturing          = false
    captureSubscribers = {}
    pendingManualSends = {}
    middlewareChain    = {}
    filterBytes        = {}

    raknet.clearrecent()
    raknet.resetstats()

    rawset(nativeRakNet, "__transportMode", nil)
end

-- exports

raknet.Capture = setmetatable({}, Capture)
getgenv().raknet = raknet
getgenv().rnet   = raknet

return raknet
