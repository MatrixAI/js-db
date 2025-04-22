import type { Crypto } from './types.js';
import type { WorkerManifest } from '@matrixai/workers';
import { expose } from '@matrixai/workers';
import nodeForge from 'node-forge';

const { random, cipher, util: forgeUtil } = nodeForge;
const ivSize = 16;
const authTagSize = 16;

function getRandomBytesSync(size: number): Buffer {
  return Buffer.from(random.getBytesSync(size), 'binary');
}

async function encrypt(
  key: ArrayBuffer,
  plainText: ArrayBuffer,
): Promise<ArrayBuffer> {
  const iv = getRandomBytesSync(ivSize);
  const c = cipher.createCipher('AES-GCM', Buffer.from(key).toString('binary'));
  c.start({ iv: iv.toString('binary'), tagLength: authTagSize * 8 });
  c.update(forgeUtil.createBuffer(plainText));
  c.finish();
  const cipherText = Buffer.from(c.output.getBytes(), 'binary');
  const authTag = Buffer.from(c.mode.tag.getBytes(), 'binary');
  const data = Buffer.concat([iv, authTag, cipherText]);
  return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
}

async function decrypt(
  key: ArrayBuffer,
  cipherText: ArrayBuffer,
): Promise<ArrayBuffer | undefined> {
  const cipherTextBuf = Buffer.from(cipherText);
  if (cipherTextBuf.byteLength < 32) {
    return;
  }
  const iv = cipherTextBuf.subarray(0, ivSize);
  const authTag = cipherTextBuf.subarray(ivSize, ivSize + authTagSize);
  const cipherText_ = cipherTextBuf.subarray(ivSize + authTagSize);
  const d = cipher.createDecipher(
    'AES-GCM',
    Buffer.from(key).toString('binary'),
  );
  d.start({
    iv: iv.toString('binary'),
    tagLength: authTagSize * 8,
    tag: forgeUtil.createBuffer(authTag),
  });
  d.update(forgeUtil.createBuffer(cipherText_));
  if (!d.finish()) {
    return;
  }
  const data = Buffer.from(d.output.getBytes(), 'binary');
  return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
}

const exampleDbWorker: Crypto = {
  async encrypt(
    {
      key,
      plainText,
    }: {
      key: ArrayBuffer;
      plainText: ArrayBuffer;
    },
    transferList: [ArrayBuffer, ArrayBuffer], // eslint-disable-line @typescript-eslint/no-unused-vars
  ): Promise<{ data: ArrayBuffer; transferList: [ArrayBuffer] }> {
    const cipherText = await encrypt(key, plainText);
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
    transferList: [ArrayBuffer, ArrayBuffer], // eslint-disable-line @typescript-eslint/no-unused-vars
  ): Promise<
    | { data: ArrayBuffer; transferList: [ArrayBuffer] }
    | { data: undefined; transferList: [] }
  > {
    const plainText = await decrypt(key, cipherText);
    if (plainText != null) {
      return { data: plainText, transferList: [plainText] };
    } else {
      return { data: undefined, transferList: [] };
    }
  },
} satisfies WorkerManifest;

expose(exampleDbWorker);

type ExampleDBWorker = typeof exampleDbWorker;

export type { ExampleDBWorker };

export default exampleDbWorker;
