import { createClient } from 'redis'
import ms from '@prsm/ms'
import { scanRecent } from './scanRecent.js'

/**
 * @typedef {Object} TokenBucketOptions
 * @property {number} capacity - maximum number of tokens the bucket can hold, which is also the largest burst a single key is allowed before it must wait for a refill.
 * @property {number} refillRate - number of tokens added back to the bucket each refill interval. The sustained allowed rate is refillRate per refillInterval.
 * @property {number|string} refillInterval - time between refills, as a duration string ("1s", "100ms") or milliseconds. Tokens are credited in whole-interval steps, so a request can wait up to one interval for the next refill.
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis] - node-redis connection options passed straight to createClient. Omit to connect to redis on localhost:6379.
 * @property {string} [prefix] - prefix applied to every Redis key this limiter writes (default "limit:tb:"). Use distinct prefixes to keep separate limiters from colliding on the same Redis instance.
 * @property {object} [tracer] - optional @prsm/trace tracer. When provided, each take() call is wrapped in a span recording the key, cost, and allowed result (default none).
 */

/**
 * @typedef {Object} TokenBucketResult
 * @property {boolean} allowed - whether the request was permitted and the tokens were consumed.
 * @property {number} remaining - tokens left in the bucket after this call.
 * @property {number} retryAfter - milliseconds until enough tokens will have refilled to satisfy the request (0 when allowed, -1 when cost exceeds capacity and the request can never succeed).
 */

const TAKE_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local refillInterval = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

local tokens = tonumber(redis.call('HGET', key, 't'))
local lastRefill = tonumber(redis.call('HGET', key, 'r'))

if tokens == nil then
  tokens = capacity
  lastRefill = now
end

local elapsed = now - lastRefill
local intervals = math.floor(elapsed / refillInterval)
if intervals > 0 then
  tokens = math.min(capacity, tokens + intervals * refillRate)
  lastRefill = lastRefill + intervals * refillInterval
end

if cost > capacity then
  return {0, tokens, -1}
end

if tokens >= cost then
  tokens = tokens - cost
  redis.call('HSET', key, 't', tokens, 'r', lastRefill)
  redis.call('PEXPIRE', key, ttl)
  return {1, tokens, 0}
end

redis.call('HSET', key, 't', tokens, 'r', lastRefill)
redis.call('PEXPIRE', key, ttl)
local deficit = cost - tokens
local intervalsNeeded = math.ceil(deficit / refillRate)
local retryAfter = intervalsNeeded * refillInterval - (now - lastRefill)
return {0, tokens, retryAfter}
`

const PEEK_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local refillInterval = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local tokens = tonumber(redis.call('HGET', key, 't'))
local lastRefill = tonumber(redis.call('HGET', key, 'r'))

if tokens == nil then
  return {capacity}
end

local elapsed = now - lastRefill
local intervals = math.floor(elapsed / refillInterval)
if intervals > 0 then
  tokens = math.min(capacity, tokens + intervals * refillRate)
end

return {tokens}
`

/**
 * @param {TokenBucketOptions} options - limiter configuration. capacity, refillRate, and refillInterval are required; redis, prefix, and tracer are optional.
 * @returns {{ take: (key: string, cost?: number) => Promise<TokenBucketResult>, peek: (key: string) => Promise<{remaining: number}>, reset: (key: string) => Promise<void>, keys: (options?: {limit?: number, scanCap?: number}) => Promise<Array<object>>, close: () => Promise<void> }}
 */
export function tokenBucket(options) {
  const { capacity, refillRate } = options
  const refillInterval = ms(options.refillInterval)
  const prefix = options.prefix ?? 'limit:tb:'
  const tracer = options.tracer ?? null

  if (!Number.isFinite(capacity) || capacity <= 0) throw new Error('capacity must be a positive number')
  if (!Number.isFinite(refillRate) || refillRate <= 0) throw new Error('refillRate must be a positive number')
  if (!Number.isFinite(refillInterval) || refillInterval <= 0)
    throw new Error('refillInterval must be a positive duration')

  const ttl = Math.ceil(capacity / refillRate) * refillInterval * 2

  const redis = createClient(options.redis ?? {})
  redis.on('error', () => {})
  const readyPromise = redis.connect()

  async function _take(key, cost = 1) {
    await readyPromise
    const result = await redis.eval(TAKE_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [
        String(capacity),
        String(refillRate),
        String(refillInterval),
        String(cost),
        String(Date.now()),
        String(ttl),
      ],
    })
    return { allowed: result[0] === 1, remaining: result[1], retryAfter: result[2] }
  }

  async function take(key, cost = 1) {
    if (!tracer) return _take(key, cost)
    return tracer.span('limit.tokenBucket.take', { 'limit.key': key, 'limit.cost': cost }, async (span) => {
      const r = await _take(key, cost)
      span.setAttribute('limit.allowed', r.allowed)
      return r
    })
  }

  async function peek(key) {
    await readyPromise
    const result = await redis.eval(PEEK_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(capacity), String(refillRate), String(refillInterval), String(Date.now())],
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

  return { take, peek, reset, keys, close }
}
