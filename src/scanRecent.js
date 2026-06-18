/**
 * Shared by all limiter types. Enumerates keys under a limiter's prefix,
 * ordered by recency, and returns the most recent N with their current state.
 *
 * Recency is derived from the Redis key TTL: every limiter PEXPIREs a key on
 * activity, so a higher remaining TTL means the key was touched more recently.
 * The SCAN is bounded by scanCap so a limiter holding 100k+ keys does not turn
 * this into an unbounded operation - it returns the most recent of a sample.
 *
 * @param {Object} ctx - limiter internals supplied by the caller.
 * @param {import('redis').RedisClientType} ctx.redis - connected node-redis client to scan.
 * @param {string} ctx.prefix - key prefix identifying this limiter's keys, stripped from each returned key.
 * @param {(key: string) => Promise<object>} ctx.peek - the limiter's peek function, used to read each key's current state without consuming it.
 * @param {{limit?: number, scanCap?: number}} [options] - listing controls.
 * @param {number} [options.limit] - maximum number of keys to return, newest first (default 25).
 * @param {number} [options.scanCap] - upper bound on how many matching keys to SCAN before ranking by recency (default 500). This keeps the operation bounded on limiters holding very large key counts, at the cost of returning the most recent of a sample rather than a true global ordering.
 * @returns {Promise<Array<object>>} recent keys, newest first, each with peek state
 */
export async function scanRecent({ redis, prefix, peek }, options = {}) {
  const limit = options.limit ?? 25
  const scanCap = options.scanCap ?? 500

  const found = []
  let cursor = '0'
  do {
    const reply = await redis.scan(cursor, { MATCH: `${prefix}*`, COUNT: 200 })
    cursor = String(reply.cursor)
    for (const fullKey of reply.keys) found.push(fullKey)
    if (found.length >= scanCap) break
  } while (cursor !== '0')

  const sample = found.slice(0, scanCap)
  const withTtl = await Promise.all(
    sample.map(async (fullKey) => ({
      key: fullKey.slice(prefix.length),
      ttl: await redis.pTTL(fullKey),
    })),
  )
  withTtl.sort((a, b) => b.ttl - a.ttl)

  const recent = []
  for (const { key } of withTtl.slice(0, limit)) {
    recent.push({ key, ...(await peek(key)) })
  }
  return recent
}
