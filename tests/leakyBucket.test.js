import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient } from 'redis'
import { leakyBucket } from '../src/leakyBucket.js'

describe('leakyBucket', () => {
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

  it('should allow requests when bucket is empty', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1s' })

    const result = await limiter.drip('k1')
    expect(result.allowed).toBe(true)
    expect(result.remaining).toBe(9)
    expect(result.retryAfter).toBe(0)
  })

  it('should fill the bucket with each drip', async () => {
    limiter = leakyBucket({ capacity: 3, drainRate: 1, drainInterval: '1s' })

    const r1 = await limiter.drip('k1')
    expect(r1.remaining).toBe(2)

    const r2 = await limiter.drip('k1')
    expect(r2.remaining).toBe(1)

    const r3 = await limiter.drip('k1')
    expect(r3.remaining).toBe(0)
  })

  it('should deny when bucket is full', async () => {
    limiter = leakyBucket({ capacity: 2, drainRate: 1, drainInterval: '1s' })

    await limiter.drip('k1')
    await limiter.drip('k1')
    const result = await limiter.drip('k1')

    expect(result.allowed).toBe(false)
    expect(result.remaining).toBe(0)
    expect(result.retryAfter).toBeGreaterThan(0)
  })

  it('should drain over time', async () => {
    limiter = leakyBucket({ capacity: 5, drainRate: 5, drainInterval: 100 })

    for (let i = 0; i < 5; i++) await limiter.drip('k1')
    const full = await limiter.drip('k1')
    expect(full.allowed).toBe(false)

    await new Promise((r) => setTimeout(r, 150))

    const drained = await limiter.drip('k1')
    expect(drained.allowed).toBe(true)
  })

  it('should not drain below zero', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 100, drainInterval: 100 })

    await limiter.drip('k1')
    await new Promise((r) => setTimeout(r, 200))

    const result = await limiter.peek('k1')
    expect(result.remaining).toBe(10)
  })

  it('should enforce smooth rate (no bursting past capacity)', async () => {
    limiter = leakyBucket({ capacity: 3, drainRate: 1, drainInterval: 100 })

    await limiter.drip('k1')
    await limiter.drip('k1')
    await limiter.drip('k1')

    const denied = await limiter.drip('k1')
    expect(denied.allowed).toBe(false)

    await new Promise((r) => setTimeout(r, 110))

    const oneSlot = await limiter.drip('k1')
    expect(oneSlot.allowed).toBe(true)

    const stillFull = await limiter.drip('k1')
    expect(stillFull.allowed).toBe(false)
  })

  it('should deny when cost exceeds capacity', async () => {
    limiter = leakyBucket({ capacity: 5, drainRate: 1, drainInterval: '1s' })

    const result = await limiter.drip('k1', 10)
    expect(result.allowed).toBe(false)
    expect(result.retryAfter).toBe(-1)
  })

  it('should support multi-drip cost', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1s' })

    const r1 = await limiter.drip('k1', 4)
    expect(r1.remaining).toBe(6)

    const r2 = await limiter.drip('k1', 6)
    expect(r2.remaining).toBe(0)

    const r3 = await limiter.drip('k1', 1)
    expect(r3.allowed).toBe(false)
  })

  it('should return accurate retryAfter', async () => {
    limiter = leakyBucket({ capacity: 1, drainRate: 1, drainInterval: 500 })

    await limiter.drip('k1')
    const immediate = await limiter.drip('k1')
    expect(immediate.allowed).toBe(false)
    expect(immediate.retryAfter).toBeLessThanOrEqual(500)
    expect(immediate.retryAfter).toBeGreaterThan(450)

    await new Promise((r) => setTimeout(r, 300))
    const later = await limiter.drip('k1')
    expect(later.allowed).toBe(false)
    expect(later.retryAfter).toBeLessThan(250)
    expect(later.retryAfter).toBeGreaterThan(0)
  })

  it('should peek without consuming', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1s' })

    const before = await limiter.peek('k1')
    expect(before.remaining).toBe(10)

    await limiter.drip('k1', 3)

    const after = await limiter.peek('k1')
    expect(after.remaining).toBe(7)
  })

  it('should reset a key', async () => {
    limiter = leakyBucket({ capacity: 2, drainRate: 1, drainInterval: '1s' })

    await limiter.drip('k1')
    await limiter.drip('k1')
    const full = await limiter.drip('k1')
    expect(full.allowed).toBe(false)

    await limiter.reset('k1')

    const fresh = await limiter.drip('k1')
    expect(fresh.allowed).toBe(true)
    expect(fresh.remaining).toBe(1)
  })

  it('should isolate keys', async () => {
    limiter = leakyBucket({ capacity: 1, drainRate: 1, drainInterval: '1s' })

    await limiter.drip('a')
    const aDenied = await limiter.drip('a')
    expect(aDenied.allowed).toBe(false)

    const bAllowed = await limiter.drip('b')
    expect(bAllowed.allowed).toBe(true)
  })

  it('should reject invalid options', () => {
    expect(() => leakyBucket({ capacity: 0, drainRate: 1, drainInterval: '1s' })).toThrow()
    expect(() => leakyBucket({ capacity: 5, drainRate: 0, drainInterval: '1s' })).toThrow()
    expect(() => leakyBucket({ capacity: 5, drainRate: 1, drainInterval: 0 })).toThrow()
    expect(() => leakyBucket({ capacity: -1, drainRate: 1, drainInterval: '1s' })).toThrow()
  })

  it('keys lists touched keys with peek state', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1m' })
    await limiter.drip('alpha')
    await limiter.drip('beta', 3)

    const keys = await limiter.keys()
    expect(keys.map((k) => k.key).sort()).toEqual(['alpha', 'beta'])
    expect(keys.find((k) => k.key === 'beta').remaining).toBe(7)
  })

  it('keys returns empty when nothing touched', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1m' })
    expect(await limiter.keys()).toEqual([])
  })

  it('keys respects the limit option', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1m' })
    for (const k of ['a', 'b', 'c', 'd']) await limiter.drip(k)
    expect(await limiter.keys({ limit: 2 })).toHaveLength(2)
  })

  it('keys drops a key after reset', async () => {
    limiter = leakyBucket({ capacity: 10, drainRate: 1, drainInterval: '1m' })
    await limiter.drip('gone')
    expect(await limiter.keys()).toHaveLength(1)
    await limiter.reset('gone')
    expect(await limiter.keys()).toEqual([])
  })
})
