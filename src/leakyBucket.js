import { createClient } from 'redis'
import ms from '@prsm/ms'
import { scanRecent } from './scanRecent.js'

/**
 * @typedef {Object} LeakyBucketOptions
 * @property {number} capacity - maximum number of requests the bucket can hold before new ones are rejected. This is the backlog tolerance, not a burst allowance.
 * @property {number} drainRate - number of requests removed from the bucket each drain interval. The sustained throughput is drainRate per drainInterval.
 * @property {number|string} drainInterval - time between drains, as a duration string ("100ms", "1s") or milliseconds. The bucket drains in whole-interval steps, so a rejected request can wait up to one interval for space.
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis] - node-redis connection options passed straight to createClient. Omit to connect to redis on localhost:6379.
 * @property {string} [prefix] - prefix applied to every Redis key this limiter writes (default "limit:lb:"). Use distinct prefixes to keep separate limiters from colliding on the same Redis instance.
 * @property {object} [tracer] - optional @prsm/trace tracer. When provided, each drip() call is wrapped in a span recording the key, cost, and allowed result (default none).
 */

/**
 * @typedef {Object} LeakyBucketResult
 * @property {boolean} allowed - whether the request was admitted into the bucket.
 * @property {number} remaining - free capacity left in the bucket after this call.
 * @property {number} retryAfter - milliseconds until the bucket drains enough to admit the request (0 when allowed, -1 when cost exceeds capacity and the request can never succeed).
 */

const DRIP_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drainRate = tonumber(ARGV[2])
local drainInterval = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

local level = tonumber(redis.call('HGET', key, 'l'))
local lastDrain = tonumber(redis.call('HGET', key, 'd'))

if level == nil then
  level = 0
  lastDrain = now
end

local elapsed = now - lastDrain
local intervals = math.floor(elapsed / drainInterval)
if intervals > 0 then
  level = math.max(0, level - intervals * drainRate)
  lastDrain = lastDrain + intervals * drainInterval
end

if cost > capacity then
  return {0, capacity - level, -1}
end

if level + cost <= capacity then
  level = level + cost
  redis.call('HSET', key, 'l', level, 'd', lastDrain)
  redis.call('PEXPIRE', key, ttl)
  return {1, capacity - level, 0}
end

redis.call('HSET', key, 'l', level, 'd', lastDrain)
redis.call('PEXPIRE', key, ttl)
local overflow = (level + cost) - capacity
local intervalsNeeded = math.ceil(overflow / drainRate)
local retryAfter = intervalsNeeded * drainInterval - (now - lastDrain)
return {0, capacity - level, retryAfter}
`

const PEEK_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drainRate = tonumber(ARGV[2])
local drainInterval = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local level = tonumber(redis.call('HGET', key, 'l'))
local lastDrain = tonumber(redis.call('HGET', key, 'd'))

if level == nil then
  return {capacity}
end

local elapsed = now - lastDrain
local intervals = math.floor(elapsed / drainInterval)
if intervals > 0 then
  level = math.max(0, level - intervals * drainRate)
end

return {capacity - level}
`

/**
 * @param {LeakyBucketOptions} options - limiter configuration. capacity, drainRate, and drainInterval are required; redis, prefix, and tracer are optional.
 * @returns {{ drip: (key: string, cost?: number) => Promise<LeakyBucketResult>, peek: (key: string) => Promise<{remaining: number}>, reset: (key: string) => Promise<void>, keys: (options?: {limit?: number, scanCap?: number}) => Promise<Array<object>>, close: () => Promise<void> }}
 */
export function leakyBucket(options) {
  const { capacity, drainRate } = options
  const drainInterval = ms(options.drainInterval)
  const prefix = options.prefix ?? 'limit:lb:'
  const tracer = options.tracer ?? null

  if (!Number.isFinite(capacity) || capacity <= 0) throw new Error('capacity must be a positive number')
  if (!Number.isFinite(drainRate) || drainRate <= 0) throw new Error('drainRate must be a positive number')
  if (!Number.isFinite(drainInterval) || drainInterval <= 0)
    throw new Error('drainInterval must be a positive duration')

  const ttl = Math.ceil(capacity / drainRate) * drainInterval * 2

  const redis = createClient(options.redis ?? {})
  redis.on('error', () => {})
  const readyPromise = redis.connect()

  async function _drip(key, cost = 1) {
    await readyPromise
    const result = await redis.eval(DRIP_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [
        String(capacity),
        String(drainRate),
        String(drainInterval),
        String(cost),
        String(Date.now()),
        String(ttl),
      ],
    })
    return { allowed: result[0] === 1, remaining: result[1], retryAfter: result[2] }
  }

  async function drip(key, cost = 1) {
    if (!tracer) return _drip(key, cost)
    return tracer.span('limit.leakyBucket.drip', { 'limit.key': key, 'limit.cost': cost }, async (span) => {
      const r = await _drip(key, cost)
      span.setAttribute('limit.allowed', r.allowed)
      return r
    })
  }

  async function peek(key) {
    await readyPromise
    const result = await redis.eval(PEEK_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(capacity), String(drainRate), String(drainInterval), String(Date.now())],
    })
    return { remaining: result[0] }
  }

  async function reset(key) {
    await readyPromise
    await redis.del(`${prefix}${key}`)
  }

  async function keys(opts) {
    await readyPromise
    return scanRecent({ redis, prefix, peek }, opts)
  }

  async function close() {
    await readyPromise.catch(() => {})
    if (redis.isOpen) await redis.quit()
  }

  return { drip, peek, reset, keys, close }
}
