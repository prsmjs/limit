import { createClient } from 'redis'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { scanRecent } from './scanRecent.js'

/**
 * @typedef {Object} SlidingWindowOptions
 * @property {number} max - maximum number of requests allowed within any rolling window. Must be a positive integer.
 * @property {number|string} window - length of the rolling window, as a duration string ("1m", "30s") or milliseconds. The limit is enforced continuously over the trailing window, not reset on fixed boundaries, so there is no burst at the window edge.
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis] - node-redis connection options passed straight to createClient. Omit to connect to redis on localhost:6379.
 * @property {string} [prefix] - prefix applied to every Redis key this limiter writes (default "limit:sw:"). Use distinct prefixes to keep separate limiters from colliding on the same Redis instance.
 * @property {object} [tracer] - optional @prsm/trace tracer. When provided, each hit() call is wrapped in a span recording the key, cost, and allowed result (default none).
 */

/**
 * @typedef {Object} SlidingWindowResult
 * @property {boolean} allowed - whether the request was permitted and recorded in the window.
 * @property {number} remaining - requests still available in the current window after this call.
 * @property {number} retryAfter - milliseconds until the oldest in-window entry expires and frees a slot (0 when allowed, -1 when cost exceeds max and the request can never succeed).
 * @property {number} total - number of requests counted in the window, including this one when allowed.
 */

const HIT_SCRIPT = `
local key = KEYS[1]
local max = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

local count = redis.call('ZCARD', key)

if cost > max then
  return {0, 0, -1, count}
end

if count + cost <= max then
  for i = 1, cost do
    redis.call('ZADD', key, now, ARGV[4 + i])
  end
  redis.call('PEXPIRE', key, window)
  return {1, max - count - cost, 0, count + cost}
end

local needed = count + cost - max
local entries = redis.call('ZRANGE', key, 0, needed - 1, 'WITHSCORES')
local retryAfter = 0
if #entries >= 2 then
  retryAfter = tonumber(entries[#entries]) + window - now
  if retryAfter < 0 then retryAfter = 0 end
end
return {0, 0, retryAfter, count}
`

const PEEK_SCRIPT = `
local key = KEYS[1]
local max = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local count = redis.call('ZCOUNT', key, now - window + 1, '+inf')
return {max - count, count}
`

/**
 * @param {SlidingWindowOptions} options - limiter configuration. max and window are required; redis, prefix, and tracer are optional.
 * @returns {{ hit: (key: string, cost?: number) => Promise<SlidingWindowResult>, peek: (key: string) => Promise<{remaining: number, total: number}>, reset: (key: string) => Promise<void>, keys: (options?: {limit?: number, scanCap?: number}) => Promise<Array<object>>, close: () => Promise<void> }}
 */
export function slidingWindow(options) {
  const { max } = options
  const window = ms(options.window)
  const prefix = options.prefix ?? 'limit:sw:'
  const tracer = options.tracer ?? null

  if (!Number.isInteger(max) || max <= 0) throw new Error('max must be a positive integer')
  if (!Number.isFinite(window) || window <= 0) throw new Error('window must be a positive duration')

  const redis = createClient(options.redis ?? {})
  redis.on('error', () => {})
  const readyPromise = redis.connect()

  async function _hit(key, cost = 1) {
    if (!Number.isInteger(cost) || cost < 1) throw new Error('cost must be a positive integer')
    await readyPromise
    const ids = Array.from({ length: cost }, () => `${Date.now()}-${randomUUID()}`)
    const result = await redis.eval(HIT_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(max), String(window), String(Date.now()), String(cost), ...ids],
    })
    return { allowed: result[0] === 1, remaining: result[1], retryAfter: result[2], total: result[3] }
  }

  async function hit(key, cost = 1) {
    if (!tracer) return _hit(key, cost)
    return tracer.span('limit.slidingWindow.hit', { 'limit.key': key, 'limit.cost': cost }, async (span) => {
      const r = await _hit(key, cost)
      span.setAttribute('limit.allowed', r.allowed)
      return r
    })
  }

  async function peek(key) {
    await readyPromise
    const result = await redis.eval(PEEK_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(max), String(window), String(Date.now())],
    })
    return { remaining: result[0], total: result[1] }
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

  return { hit, peek, reset, keys, close }
}
