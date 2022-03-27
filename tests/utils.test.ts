import type { KeyPath } from '@/types';
import * as utils from '@/utils';

describe('utils', () => {
  test('parse key paths', () => {
    // The key actual is allowed to contain the separator buffer
    // However levels are not allowed for this
    const keyPaths: Array<KeyPath> = [
      ['foo', 'bar', Buffer.concat([utils.sep, Buffer.from('key'), utils.sep])],
      [utils.sep],
      [Buffer.concat([utils.sep, Buffer.from('foobar')])],
      [Buffer.concat([Buffer.from('foobar'), utils.sep])],
      [Buffer.concat([utils.sep, Buffer.from('foobar'), utils.sep])],
    ];
    for (const keyPath of keyPaths) {
      const key = utils.keyPathToKey(keyPath);
      const keyPath_ = utils.parseKey(key);
      expect(keyPath.map((b) => b.toString())).toStrictEqual(
        keyPath_.map((b) => b.toString()),
      );
    }
  });
});
