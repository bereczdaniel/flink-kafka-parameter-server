local userKey = KEYS[1]
local destinationId = ARGV[1]
local channelName = ARGV[2]
local numFactors = ARGV[3] -- vector length
local randomMin = ARGV[4]
local randomMax = ARGV[5]

--local floatFormatString = "%.15E"
local floatFormatString = "%.17f"   -- redis precision

--puts flat_map { 0, val0, 2, val2, 1, val1, ... } into the result_table 
--  as { val0, val1, val2, ... } starting from result_start_index
local function packToArray(flatMap, resultTable, resultStartIdx)
    for i = 1, #flatMap, 2 do
        resultTable[flatMap[i] + resultStartIdx] = flatMap[i + 1]
    end
end

local function randomFloat(min, max)
    return min + math.random() * (max - min);
end

-- sets the nonexistent values of the vector at key in db to random values
local function setRandomVector(key, size, min, max)
    math.randomseed(key)
    for i = 0, size-1, 1 do
        redis.call('HSETNX', key, i, string.format(floatFormatString, randomFloat(min, max)))
    end
end

-- message string format: "userKey(sourceId=userId),destinationId(=evaluationId),val0,val2,val3,..."

local flatMap = redis.call('HGETALL', userKey)
if #flatMap == 0 then
    setRandomVector(userKey, numFactors, randomMin, randomMax)
    flatMap = redis.call('HGETALL', userKey)
end
local vectorArr = { }
packToArray(flatMap, vectorArr, 1)
local messageArr = { userKey, destinationId, table.concat(vectorArr,",") }
redis.call('PUBLISH', channelName, table.concat(messageArr,":"))
