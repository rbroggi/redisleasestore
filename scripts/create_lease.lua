local key = KEYS[1]
local holderIdentity = ARGV[1]
local acquireTime = ARGV[2]
local renewTime = ARGV[3]
local leaseDuration = ARGV[4]
local leaderTransitions = ARGV[5]

local currentHolder = redis.call('HGET', key, 'HolderIdentity')

if currentHolder == false then
    redis.call('HSET', key, 'HolderIdentity', holderIdentity)
    redis.call('HSET', key, 'AcquireTime', acquireTime)
    redis.call('HSET', key, 'RenewTime', renewTime)
    redis.call('HSET', key, 'LeaseDuration', leaseDuration)
    redis.call('HSET', key, 'LeaderTransitions', leaderTransitions)
    return 1  -- Success
else
    return 0  -- Failed: already exists
end
