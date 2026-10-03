-- Removes every block at or below `up_to`, canonical and side blocks, PRUNE_HEIGHTS_PER_CALL
-- heights at a time, and emits `pruned` once none are left.
-- Reply: { done (0 or 1), blocks removed by this call }.
local up_to_number, up_to_hash = ARGV[1], ARGV[2]

local heights = redis.call('ZRANGEBYSCORE', heights_key, '-inf', up_to_number,
  'LIMIT', 0, PRUNE_HEIGHTS_PER_CALL)
local removed = 0
for _, height in ipairs(heights) do
  removed = removed + remove_height(tonumber(height))
end
if redis.call('ZCOUNT', heights_key, '-inf', up_to_number) > 0 then
  return { 0, removed }
end

emit({}, { 'type', 'pruned', 'number', up_to_number, 'hash', up_to_hash })
return { 1, removed }
