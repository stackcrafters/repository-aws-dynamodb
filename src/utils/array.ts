export const chunkArray = (arr: any[], size: number): any[] => {
  if (!Array.isArray(arr)) {
    return [];
  }
  return Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));
};

export default { chunkArray };
