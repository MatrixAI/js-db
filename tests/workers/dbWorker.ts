import type { Crypto } from '#types.js';
import type { WorkerManifest } from '@matrixai/workers';
import { expose } from '@matrixai/workers';
import * as utils from '../utils.js';

const dbWorker: Crypto = {
  async encrypt(
    {
      key,
      plainText,
    }: {
      key: ArrayBuffer;
      plainText: ArrayBuffer;
    },
    // eslint-disable-line @typescript-eslint/no-unused-vars
    transferList: [ArrayBuffer, ArrayBuffer],
  ): Promise<{ data: ArrayBuffer; transferList: [ArrayBuffer] }> {
    const cipherText = await utils.encrypt(key, plainText);
    return { data: cipherText, transferList: [cipherText] };
  },
  async decrypt(
    {
      key,
      cipherText,
    }: {
      key: ArrayBuffer;
      cipherText: ArrayBuffer;
    },
    // eslint-disable-line @typescript-eslint/no-unused-vars
    transferList: [ArrayBuffer, ArrayBuffer],
  ): Promise<
    | { data: ArrayBuffer; transferList: [ArrayBuffer] }
    | { data: undefined; transferList: [] }
  > {
    const plainText = await utils.decrypt(key, cipherText);
    if (plainText != null) {
      return { data: plainText, transferList: [plainText] };
    } else {
      return { data: undefined, transferList: [] };
    }
  },
} satisfies WorkerManifest;

expose(dbWorker);

type DBWorker = typeof dbWorker;

export type { DBWorker };

export default dbWorker;
