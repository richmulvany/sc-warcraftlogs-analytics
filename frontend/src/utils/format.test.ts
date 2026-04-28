import { describe, it, expect } from 'vitest'
import { toFiniteNumber, meanIgnoringNulls } from './format'

describe('toFiniteNumber', () => {
  it('returns null for null', () => expect(toFiniteNumber(null)).toBeNull())
  it('returns null for undefined', () => expect(toFiniteNumber(undefined)).toBeNull())
  it('returns null for empty string', () => expect(toFiniteNumber('')).toBeNull())
  it('returns null for NaN string', () => expect(toFiniteNumber('abc')).toBeNull())
  it('returns null for Infinity', () => expect(toFiniteNumber(Infinity)).toBeNull())
  it('returns 0 for 0 (not null — zero is a real value)', () => expect(toFiniteNumber(0)).toBe(0))
  it('returns the number for a valid integer', () => expect(toFiniteNumber(42)).toBe(42))
  it('returns the number for a numeric string', () => expect(toFiniteNumber('3.14')).toBeCloseTo(3.14))
  it('returns negative numbers', () => expect(toFiniteNumber(-5)).toBe(-5))
})

describe('meanIgnoringNulls', () => {
  it('returns 0 for an empty array', () => expect(meanIgnoringNulls([])).toBe(0))
  it('returns 0 for an all-null array', () => expect(meanIgnoringNulls([null, undefined])).toBe(0))
  it('ignores nulls and computes mean of finite values', () =>
    expect(meanIgnoringNulls([null, undefined, 10, 20])).toBe(15))
  it('computes mean of a plain numeric array', () => expect(meanIgnoringNulls([2, 4, 6])).toBe(4))
  it('ignores NaN entries', () => expect(meanIgnoringNulls([NaN, 8, 12])).toBe(10))
})
