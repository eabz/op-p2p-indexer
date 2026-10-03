-- Stores a block and applies fork choice (docs/storage.md section 3.2), atomically.
-- Reply: { status, events }. Status 1: stored. 0: not stored, it was already stored or is at or
-- below the safe head. -1: rejected, its number contradicts its parent's. Each event is a flat
-- list of field names and values.
local number, hash, parent_hash, timestamp = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
local header, transactions, receipts = ARGV[5], ARGV[6], ARGV[7]
local source, received_at_ms, tx_count = ARGV[8], ARGV[9], ARGV[10]

-- L1 has committed everything at or below the safe head; -1 until one is known.
local safe = redis.call('HMGET', safe_head_key, 'number', 'hash')
local safe_number, safe_hash = tonumber(safe[1]) or -1, safe[2]

-- Returns the canonical hash at `height`, or nil for a gap.
local function canonical_at(height)
  return redis.call('ZRANGEBYSCORE', canonical_key, num(height), num(height))[1]
end

-- Returns the number and parent hash of a stored block, or nil if it is not stored.
local function stored(block_hash)
  local fields = redis.call('HMGET', block_key(block_hash), 'number', 'parent_hash')
  if fields[1] then
    return { number = tonumber(fields[1]), parent = fields[2] }
  end
end

-- Returns the parent hash of `block_hash` if it is stored at `height`. The walks use this, so
-- they stop at a block that is missing or whose stored number is not the height they computed.
local function parent_at(block_hash, height)
  local block = stored(block_hash)
  return block and block.number == height and block.parent
end

local function set_canonical(height, block_hash)
  redis.call('ZADD', canonical_key, num(height), block_hash)
end

-- Removes the canonical entries at `height` and above, and returns their hashes, newest
-- first. The callers bound the range by MAX_REORG_DEPTH.
local function remove_canonical_from(height)
  local removed = redis.call('ZREVRANGEBYSCORE', canonical_key, '+inf', num(height))
  redis.call('ZREMRANGEBYSCORE', canonical_key, num(height), '+inf')
  return removed
end

-- Removes one canonical entry and records it in `replaced`.
local function replace(replaced, block_hash)
  replaced[#replaced + 1] = block_hash
  redis.call('ZREM', canonical_key, block_hash)
end

-- Makes the new block the head and emits `head`.
local function move_head(out, gap)
  set_canonical(number, hash)
  redis.call('HSET', head_key, 'number', num(number), 'hash', hash, 'timestamp', timestamp)
  emit(out, {
    'type', 'head', 'number', num(number), 'hash', hash, 'parent_hash', parent_hash,
    'timestamp', timestamp, 'gap', gap and '1' or '0',
  })
end

-- Emits `reorg`. `replaced` is newest first; `ancestor` is nil when the replaced range ends
-- in a gap.
local function emit_reorg(out, ancestor, old_head, new_head, replaced)
  local fields = { 'type', 'reorg' }
  if ancestor then
    append(fields, { 'ancestor_number', num(ancestor.number), 'ancestor_hash', ancestor.hash })
  end
  append(fields, {
    'old_head_number', num(old_head.number), 'old_head_hash', old_head.hash,
    'new_head_number', num(new_head.number), 'new_head_hash', new_head.hash,
    'replaced', table.concat(replaced, ','),
  })
  emit(out, fields)
end

-- Whether the block `block_hash` at `height` is where a walk meets the canonical chain:
-- it is `canonical`, the entry at that height, or the height has no entry and it is the safe
-- head, which stays the ancestor after it has been pruned.
local function is_ancestor(block_hash, height, canonical)
  return canonical == block_hash
    or (not canonical and height == safe_number and block_hash == safe_hash)
end

-- Step 6: the new block is two or more heights above the head and becomes the head. The stored
-- part of its ancestry becomes canonical with it, replacing whatever else held those heights;
-- heights it cannot account for stay a gap. Nothing at or below the safe height is touched.
local function jump(out, head)
  -- Stored ancestors of the new block, highest first; `cursor` at `height` is the next one
  -- and `below` the canonical entry at that height.
  local path = {}
  local cursor, height = parent_hash, number - 1
  local below = canonical_at(height)
  for _ = 1, MAX_REORG_DEPTH do
    local parent = not is_ancestor(cursor, height, below) and height > safe_number
      and parent_at(cursor, height)
    if not parent then
      break
    end
    path[#path + 1] = { number = height, hash = cursor }
    cursor, height = parent, height - 1
    below = canonical_at(height)
  end

  local ancestor = is_ancestor(cursor, height, below) and { number = height, hash = cursor }
  -- The walk reached the safe height without meeting the chain: the block above it does not
  -- build on what L1 committed, whether or not the safe block is still stored.
  local contradicts_l1 = not ancestor and height <= safe_number
  if contradicts_l1 and #path == 0 then
    -- That block is the new one itself: it stays a side block.
    return
  end

  local replaced = remove_canonical_from(height + 1)
  if contradicts_l1 then
    -- The lowest block of the path stays out; its height is a gap.
    path[#path] = nil
  elseif not ancestor and below then
    -- The path ends at a block whose parent, `cursor`, is unknown, so the entry below it
    -- belongs to another branch.
    replace(replaced, below)
  end
  for _, block in ipairs(path) do
    set_canonical(block.number, block.hash)
  end
  if #replaced > 0 then
    emit_reorg(out, ancestor or nil, head, { number = number, hash = hash }, replaced)
  end
  move_head(out, not ancestor)
end

-- Step 7: the new block closes a gap below the head. Fills downward through stored parents,
-- replacing or removing entries that turn out to belong to another branch, but never one at
-- or below the safe height. Wherever it stops, the entry below the last block it wrote is
-- that block's parent or absent.
local function fill(out, head)
  local replaced = {}
  local ancestor
  -- The block to make canonical next, its height, its parent, and the entry at its height.
  local block, height, parent = hash, number, parent_hash
  local previous
  for step = 1, MAX_REORG_DEPTH do
    local below = canonical_at(height - 1)
    local committed = height - 1 <= safe_number
    if previous then
      replace(replaced, previous)
    end
    -- A block whose parent is not what L1 committed stays out: its height is a gap. That
    -- holds whether or not the safe block is still stored.
    if committed and not is_ancestor(parent, height - 1, below) then
      break
    end
    set_canonical(height, block)
    emit(out, { 'type', 'fill', 'number', num(height), 'hash', block })

    if is_ancestor(parent, height - 1, below) then
      ancestor = { number = height - 1, hash = parent }
      break
    end
    -- At the last step nothing more is written, so no entry is left unchecked against the
    -- one below it.
    local grandparent = not committed and step < MAX_REORG_DEPTH and parent_at(parent, height - 1)
    if not grandparent then
      if below then
        replace(replaced, below)
      end
      break
    end
    block, height, parent, previous = parent, height - 1, grandparent, below
  end
  if #replaced > 0 then
    emit_reorg(out, ancestor, head, head, replaced)
  end
end

-- Steps 4 to 8: decides whether the stored block becomes canonical.
local function fork_choice(out, head)
  if not head or parent_hash == head.hash then
    move_head(out, false)
  elseif number >= head.number + 2 then
    jump(out, head)
  elseif number <= head.number and not canonical_at(number) then
    local above = canonical_at(number + 1)
    if above and parent_at(above, number + 1) == hash then
      fill(out, head)
    end
  end
end

-- Retention: block keys expire on their own but the sorted sets do not, and nothing prunes
-- until an L1 source exists. Drops a few of the heights further than UNSAFE_RETENTION_BLOCKS
-- below the head, with their blocks, on every insert. Emits no event.
local function retain()
  local horizon = tonumber(redis.call('HGET', head_key, 'number')) - UNSAFE_RETENTION_BLOCKS
  local expired = redis.call('ZRANGEBYSCORE', heights_key, '-inf', '(' .. num(horizon),
    'LIMIT', 0, RETENTION_HEIGHTS_PER_INSERT)
  for _, height in ipairs(expired) do
    remove_height(tonumber(height))
  end
end

if redis.call('EXISTS', block_key(hash)) == 1 or number <= safe_number then
  return { 0, {} }
end
local stored_head = redis.call('HMGET', head_key, 'number', 'hash')
local head = stored_head[1] and { number = tonumber(stored_head[1]), hash = stored_head[2] }
-- The parent's number is known if the parent is stored, or is the head (which can outlive its
-- block). A block that is not exactly one above it is rejected.
local parent = stored(parent_hash) or (head and head.hash == parent_hash and head)
if parent and parent.number ~= number - 1 then
  return { -1, {} }
end

local fields = {
  'number', num(number), 'parent_hash', parent_hash, 'timestamp', timestamp,
  'header', header, 'transactions', transactions, 'tx_count', tx_count, 'source', source,
  'received_at_ms', received_at_ms,
}
if receipts ~= '' then
  append(fields, { 'receipts', receipts })
end
redis.call('HSET', block_key(hash), unpack(fields))
redis.call('EXPIRE', block_key(hash), UNSAFE_TTL_SECS)
redis.call('SADD', height_key(number), hash)
redis.call('EXPIRE', height_key(number), UNSAFE_TTL_SECS)
redis.call('ZADD', heights_key, num(number), num(number))

local events = {}
fork_choice(events, head)
retain()
return { 1, events }
