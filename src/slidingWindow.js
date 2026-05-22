import { createClient } from 'redis'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { scanRecent } from './scanRecent.js'

/**
 * @typedef {Object} SlidingWindowOptions
 * @property {number} max - max requests per window
 * @property {number|string} window - rolling window size, ms or string like "1m"
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis] - redis connection options
 * @property {string} [prefix] - key prefix (default "limit:sw:")
 */

/**
 * @typedef {Object} SlidingWindowResult
 * @property {boolean} allowed
 * @property {number} remaining
 * @property {number} retryAfter - ms until the oldest entry expires (0 if allowed)
 * @property {number} total - current count of requests in the window
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
 * @param {SlidingWindowOptions} options
 * @returns {{ hit: (key: string, cost?: number) => Promise<SlidingWindowResult>, peek: (key: string) => Promise<{remaining: number, total: number}>, reset: (key: string) => Promise<void>, keys: (options?: {limit?: number, scanCap?: number}) => Promise<Array<object>>, close: () => Promise<void> }}
 */
export function slidingWindow(options) {
  const { max } = options
  const window = ms(options.window)
  const prefix = options.prefix ?? 'limit:sw:'

  if (!Number.isInteger(max) || max <= 0) throw new Error('max must be a positive integer')
  if (!Number.isFinite(window) || window <= 0) throw new Error('window must be a positive duration')

  const redis = createClient(options.redis ?? {})
  redis.on('error', () => {})
  const readyPromise = redis.connect()

  async function hit(key, cost = 1) {
    if (!Number.isInteger(cost) || cost < 1) throw new Error('cost must be a positive integer')
    await readyPromise
    const ids = Array.from({ length: cost }, () => `${Date.now()}-${randomUUID()}`)
    const result = await redis.eval(HIT_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(max), String(window), String(Date.now()), String(cost), ...ids],
    })
    return { allowed: result[0] === 1, remaining: result[1], retryAfter: result[2], total: result[3] }
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
