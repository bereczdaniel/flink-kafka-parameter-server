local userKey = KEYS[1]
local userDelta = ARGV

for i=1,#userDelta,1 do
    redis.call("HINCRBYFLOAT", userKey, i-1, userDelta[i])
end

