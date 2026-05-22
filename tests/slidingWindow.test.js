import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient } from 'redis'
import { slidingWindow } from '../src/slidingWindow.js'

describe('slidingWindow', () => {
  let limiter
  let redis

  beforeEach(async () => {
    redis = createClient()
    await redis.connect()
    await redis.flushAll()
  })

  afterEach(async () => {
    if (limiter) await limiter.close()
    if (redis?.isOpen) await redis.quit()
  })

  it('should allow requests under the limit', async () => {
    limiter = slidingWindow({ max: 5, window: '1s' })

    const result = await limiter.hit('k1')
    expect(result.allowed).toBe(true)
    expect(result.remaining).toBe(4)
    expect(result.total).toBe(1)
    expect(result.retryAfter).toBe(0)
  })

  it('should count hits toward the limit', async () => {
    limiter = slidingWindow({ max: 3, window: '1s' })

    const r1 = await limiter.hit('k1')
    expect(r1.total).toBe(1)
    expect(r1.remaining).toBe(2)

    const r2 = await limiter.hit('k1')
    expect(r2.total).toBe(2)
    expect(r2.remaining).toBe(1)

    const r3 = await limiter.hit('k1')
    expect(r3.total).toBe(3)
    expect(r3.remaining).toBe(0)
  })

  it('should deny when limit is reached', async () => {
    limiter = slidingWindow({ max: 2, window: '1s' })

    await limiter.hit('k1')
    await limiter.hit('k1')
    const result = await limiter.hit('k1')

    expect(result.allowed).toBe(false)
    expect(result.remaining).toBe(0)
    expect(result.total).toBe(2)
    expect(result.retryAfter).toBeGreaterThan(0)
  })

  it('should allow again after window expires', async () => {
    limiter = slidingWindow({ max: 2, window: 100 })

    await limiter.hit('k1')
    await limiter.hit('k1')
    const denied = await limiter.hit('k1')
    expect(denied.allowed).toBe(false)

    await new Promise((r) => setTimeout(r, 150))

    const allowed = await limiter.hit('k1')
    expect(allowed.allowed).toBe(true)
  })

  it('should slide the window (entries expire individually)', async () => {
    limiter = slidingWindow({ max: 2, window: 200 })

    await limiter.hit('k1')
    await new Promise((r) => setTimeout(r, 100))
    await limiter.hit('k1')

    const denied = await limiter.hit('k1')
    expect(denied.allowed).toBe(false)

    // first entry expires at ~200ms, wait for it
    await new Promise((r) => setTimeout(r, 150))

    const allowed = await limiter.hit('k1')
    expect(allowed.allowed).toBe(true)
    expect(allowed.total).toBe(2)
  })

  it('should return accurate retryAfter', async () => {
    limiter = slidingWindow({ max: 1, window: 500 })

    await limiter.hit('k1')
    const denied = await limiter.hit('k1')

    expect(denied.allowed).toBe(false)
    expect(denied.retryAfter).toBeGreaterThan(0)
    expect(denied.retryAfter).toBeLessThanOrEqual(500)
  })

  it('should deny when cost exceeds max', async () => {
    limiter = slidingWindow({ max: 5, window: '1s' })

    const result = await limiter.hit('k1', 10)
    expect(result.allowed).toBe(false)
    expect(result.retryAfter).toBe(-1)
  })

  it('should support multi-hit cost', async () => {
    limiter = slidingWindow({ max: 5, window: '1s' })

    const r1 = await limiter.hit('k1', 3)
    expect(r1.allowed).toBe(true)
    expect(r1.remaining).toBe(2)
    expect(r1.total).toBe(3)

    const r2 = await limiter.hit('k1', 3)
    expect(r2.allowed).toBe(false)
  })

  it('should peek without consuming', async () => {
    limiter = slidingWindow({ max: 10, window: '1s' })

    const before = await limiter.peek('k1')
    expect(before.remaining).toBe(10)
    expect(before.total).toBe(0)

    await limiter.hit('k1')
    await limiter.hit('k1')
    await limiter.hit('k1')

    const after = await limiter.peek('k1')
    expect(after.remaining).toBe(7)
    expect(after.total).toBe(3)
  })

  it('should reset a key', async () => {
    limiter = slidingWindow({ max: 2, window: '1s' })

    await limiter.hit('k1')
    await limiter.hit('k1')
    const denied = await limiter.hit('k1')
    expect(denied.allowed).toBe(false)

    await limiter.reset('k1')

    const fresh = await limiter.hit('k1')
    expect(fresh.allowed).toBe(true)
    expect(fresh.remaining).toBe(1)
    expect(fresh.total).toBe(1)
  })

  it('should return accurate retryAfter with multi-hit cost', async () => {
    limiter = slidingWindow({ max: 3, window: 1000 })

    await limiter.hit('k1')
    await new Promise((r) => setTimeout(r, 200))
    await limiter.hit('k1')
    await new Promise((r) => setTimeout(r, 200))
    await limiter.hit('k1')

    const denied = await limiter.hit('k1', 2)
    expect(denied.allowed).toBe(false)
    expect(denied.retryAfter).toBeGreaterThan(650)
    expect(denied.retryAfter).toBeLessThanOrEqual(1000)
  })

  it('should isolate keys', async () => {
    limiter = slidingWindow({ max: 1, window: '1s' })

    await limiter.hit('a')
    const aDenied = await limiter.hit('a')
    expect(aDenied.allowed).toBe(false)

    const bAllowed = await limiter.hit('b')
    expect(bAllowed.allowed).toBe(true)
  })

  it('should reject invalid options', () => {
    expect(() => slidingWindow({ max: 0, window: '1s' })).toThrow()
    expect(() => slidingWindow({ max: 5, window: 0 })).toThrow()
    expect(() => slidingWindow({ max: 1.5, window: '1s' })).toThrow()
    expect(() => slidingWindow({ max: -1, window: '1s' })).toThrow()
  })

  it('should reject invalid cost', async () => {
    limiter = slidingWindow({ max: 5, window: '1s' })
    await expect(limiter.hit('k1', 1.5)).rejects.toThrow()
    await expect(limiter.hit('k1', 0)).rejects.toThrow()
  })

  it('keys lists touched keys with peek state', async () => {
    limiter = slidingWindow({ max: 5, window: '1m' })
    await limiter.hit('alpha')
    await limiter.hit('beta')
    await limiter.hit('beta')

    const keys = await limiter.keys()
    expect(keys.map((k) => k.key).sort()).toEqual(['alpha', 'beta'])
    const beta = keys.find((k) => k.key === 'beta')
    expect(beta.total).toBe(2)
    expect(beta.remaining).toBe(3)
  })

  it('keys returns empty when nothing touched', async () => {
    limiter = slidingWindow({ max: 5, window: '1m' })
    expect(await limiter.keys()).toEqual([])
  })

  it('keys orders most-recently-touched first', async () => {
    limiter = slidingWindow({ max: 5, window: '1m' })
    await limiter.hit('older')
    await new Promise((r) => setTimeout(r, 20))
    await limiter.hit('newer')

    const keys = await limiter.keys()
    expect(keys[0].key).toBe('newer')
  })

  it('keys respects the limit option', async () => {
    limiter = slidingWindow({ max: 5, window: '1m' })
    for (const k of ['a', 'b', 'c', 'd']) await limiter.hit(k)
    const keys = await limiter.keys({ limit: 2 })
    expect(keys).toHaveLength(2)
  })

  it('keys drops a key after reset', async () => {
    limiter = slidingWindow({ max: 5, window: '1m' })
    await limiter.hit('gone')
    expect(await limiter.keys()).toHaveLength(1)
    await limiter.reset('gone')
    expect(await limiter.keys()).toEqual([])
  })
})
