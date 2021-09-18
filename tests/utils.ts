import { random, cipher, util as forgeUtil } from 'node-forge';

const ivSize = 16;
const authTagSize = 16;

async function getRandomBytes(size: number): Promise<Buffer> {
  return Buffer.from(await random.getBytes(size), 'binary');
}

function getRandomBytesSync(size: number): Buffer {
  return Buffer.from(random.getBytesSync(size), 'binary');
}

async function generateKey(bits: 128 | 192 | 256 = 256): Promise<Buffer> {
  if (![128, 192, 256].includes(bits)) {
    throw new RangeError('AES only allows 128, 192, 256 bit sizes');
  }
  const len = Math.floor(bits / 8);
  const key = await getRandomBytes(len);
  return key;
}

function generateKeySync(bits: 128 | 192 | 256 = 256): Buffer {
  if (![128, 192, 256].includes(bits)) {
    throw new RangeError('AES only allows 128, 192, 256 bit sizes');
  }
  const len = Math.floor(bits / 8);
  const key = getRandomBytesSync(len);
  return key;
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
  if (cipherTextBuf.byteLength <= 32) {
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

export {
  getRandomBytes,
  getRandomBytesSync,
  generateKey,
  generateKeySync,
  encrypt,
  decrypt,
};