import { chunkArray } from './array';

describe('chunkArray', () => {
  it('splits an array into equal sized chunks', () => {
    const res = chunkArray([1, 2, 3, 4, 5, 6], 3);

    expect(res.length).toBe(2);
    expect(res[0]).toEqual([1, 2, 3]);
    expect(res[1]).toEqual([4, 5, 6]);
  });
  it('splits an array into chunks where last chunk is smaller', () => {
    const res = chunkArray([1, 2, 3, 4, 5, 6, 7], 3);

    expect(res.length).toBe(3);
    expect(res[2]).toEqual([7]);
  });
  it('returns an array equal to input length when chunk size is larger than input array length', () => {
    const res = chunkArray([1], 3);

    expect(res.length).toBe(1);
    expect(res[0]).toEqual([1]);
  });
  describe('returns an empty array', () => {
    it('for empty input', () => {
      const res = chunkArray([], 2);
      expect(res.length).toBe(0);
    });
    // it('for null input', () => {
    //   const res = chunkArray(null, 2);
    //   expect(res.length).toBe(0);
    // });
    // it('for undefined input', () => {
    //   const res = chunkArray(undefined, 2);
    //   expect(res.length).toBe(0);
    // });
    // it('for non-array (boolean) input', () => {
    //   const res = chunkArray(true, 2);
    //   expect(res.length).toBe(0);
    // });
  });
});
