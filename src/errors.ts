import { AbstractError } from '@matrixai/errors';

class ErrorDB<T> extends AbstractError<T> {
  static description = 'DB error';
}

class ErrorDBRunning<T> extends ErrorDB<T> {
  static desription = 'DB is running';
}

class ErrorDBNotRunning<T> extends ErrorDB<T> {
  static description = 'DB is not running';
}

class ErrorDBDestroyed<T> extends ErrorDB<T> {
  static description = 'DB is destroyed';
}

class ErrorDBCreate<T> extends ErrorDB<T> {
  static description = 'DB cannot be created';
}

class ErrorDBDelete<T> extends ErrorDB<T> {
  static description = 'DB cannot be deleted';
}

class ErrorDBKey<T> extends ErrorDB<T> {
  static description = 'DB key is incorrect';
}

class ErrorDBDecrypt<T> extends ErrorDB<T> {
  static description = 'DB failed decryption';
}

class ErrorDBParseKey<T> extends ErrorDB<T> {
  static description = 'DB key parsing failed';
}

class ErrorDBParseValue<T> extends ErrorDB<T> {
  static description = 'DB value parsing failed';
}

class ErrorDBTransactionDestroyed<T> extends ErrorDB<T> {
  static description = 'DBTransaction is destroyed';
}

class ErrorDBTransactionCommitted<T> extends ErrorDB<T> {
  static description = 'DBTransaction is committed';
}

class ErrorDBTransactionNotCommited<T> extends ErrorDB<T> {
  static description = 'DBTransaction is not comitted';
}

class ErrorDBTransactionRollbacked<T> extends ErrorDB<T> {
  static description = 'DBTransaction is rollbacked';
}

export {
  ErrorDB,
  ErrorDBRunning,
  ErrorDBNotRunning,
  ErrorDBDestroyed,
  ErrorDBCreate,
  ErrorDBDelete,
  ErrorDBKey,
  ErrorDBDecrypt,
  ErrorDBParseKey,
  ErrorDBParseValue,
  ErrorDBTransactionDestroyed,
  ErrorDBTransactionCommitted,
  ErrorDBTransactionNotCommited,
  ErrorDBTransactionRollbacked,
};
