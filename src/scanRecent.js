/**
 * Shared by all limiter types. Enumerates keys under a limiter's prefix,
 * ordered by recency, and returns the most recent N with their current state.
 *
 * Recency is derived from the Redis key TTL: every limiter PEXPIREs a key on
 * activity, so a higher remaining TTL means the key was touched more recently.
 * The SCAN is bounded by scanCap so a limiter holding 100k+ keys does not turn
 * this into an unbounded operation - it returns the most recent of a sample.
 *
 * @param {Object} ctx
 * @param {import('redis').RedisClientType} ctx.redis
 * @param {string} ctx.prefix
 * @param {(key: string) => Promise<object>} ctx.peek
 * @param {{limit?: number, scanCap?: number}} [options]
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
