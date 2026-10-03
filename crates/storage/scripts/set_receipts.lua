-- Attaches receipts to a block if it is still stored.
-- Reply: 1 written; 0 the block is not stored; -1 the receipts are not one per transaction;
-- -2 the block is stored with another number.
local number, hash, receipts, count = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

local block = redis.call('HMGET', block_key(hash), 'number', 'tx_count')
if not block[1] then
  return 0
elseif block[1] ~= number then
  return -2
elseif block[2] ~= count then
  return -1
end
redis.call('HSET', block_key(hash), 'receipts', receipts)
emit({}, { 'type', 'receipts', 'number', number, 'hash', hash })
return 1
