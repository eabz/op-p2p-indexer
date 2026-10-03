-- Prepended to every unsafe-store script, after a line of constants generated from the Rust
-- ones. Keys come from the Rust layout module (docs/storage.md section 3.1); block and height keys
-- are a prefix plus a hash or a number.
local block_prefix, height_prefix = KEYS[1], KEYS[2]
local canonical_key, head_key, safe_head_key, events_key = KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local heights_key = KEYS[7]

-- Formats a block number without the exponent `tostring` uses for large values.
local function num(n)
  return string.format('%.0f', n)
end

local function block_key(hash)
  return block_prefix .. hash
end

local function height_key(number)
  return height_prefix .. num(number)
end

local function append(list, items)
  for _, item in ipairs(items) do
    list[#list + 1] = item
  end
end

-- Removes the blocks stored at `height`, at most REMOVE_BLOCKS_PER_STEP of them. Once none are
-- left the height leaves the index and the canonical chain. Returns how many blocks it removed.
local function remove_height(height)
  local removed = 0
  for _, hash in ipairs(redis.call('SPOP', height_key(height), REMOVE_BLOCKS_PER_STEP)) do
    removed = removed + redis.call('DEL', block_key(hash))
  end
  if redis.call('EXISTS', height_key(height)) == 0 then
    redis.call('ZREM', heights_key, num(height))
    redis.call('ZREMRANGEBYSCORE', canonical_key, num(height), num(height))
  end
  return removed
end

-- Appends an event to the stream and to `out`, the script's reply.
local function emit(out, fields)
  redis.call('XADD', events_key, 'MAXLEN', '~', EVENTS_MAXLEN, '*', unpack(fields))
  out[#out + 1] = fields
end
