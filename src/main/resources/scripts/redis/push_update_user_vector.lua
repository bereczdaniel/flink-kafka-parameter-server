local userKey = KEYS[1]
local userDelta = ARGV

--No existence checking needed: if the target value is not set yet, it is initialized to 0 by redis.
for i=1,#userDelta,1 do
    redis.call("HINCRBYFLOAT", userKey, i-1, userDelta[i])
end

