-- Configuration
local SOCKET_PATH = "/var/run/telephony/telephony.sock"

-- Function to get current timestamp
function get_timestamp()
    return os.date("%Y-%m-%d %H:%M:%S")
end

-- Function to send event
function send_event()
    local socket = require("socket")
    local unix = require("socket.unix")
    local conn = unix()
    conn:settimeout(1) -- 1 second timeout

    local success, err = conn:connect(SOCKET_PATH)
    if not success then
        freeswitch.consoleLog("err", string.format("[%s] [EVENT_SYNC] Failed to connect to Unix socket: %s\n", get_timestamp(), tostring(err)))
        return false
    end

    -- Get event as JSON
    local json = event:serialize("json")
    if not json then
        freeswitch.consoleLog("err", string.format("[%s] [EVENT_SYNC] Failed to serialize event to JSON\n", get_timestamp()))
        conn:close()
        return false
    end

    -- Send event
    success, err = conn:send(json .. "\n")
    if not success then
        freeswitch.consoleLog("err", string.format("[%s] [EVENT_SYNC] Failed to send event: %s\n", get_timestamp(), tostring(err)))
        conn:close()
        return false
    end

    conn:close()
    return true
end

send_event()
