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

class ErrorDBIterator<T> extends ErrorDB<T> {
  static description = 'DBIterator error';
}

class ErrorDBIteratorDestroyed<T> extends ErrorDBIterator<T> {
  static description = 'DBIterator is destroyed';
}

class ErrorDBIteratorBusy<T> extends ErrorDBIterator<T> {
  static description = 'DBIterator is busy';
}

class ErrorDBTransaction<T> extends ErrorDB<T> {
  static description = 'DBTransaction error';
}

class ErrorDBTransactionDestroyed<T> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction is destroyed';
}

class ErrorDBTransactionCommitted<T> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction is committed';
}

class ErrorDBTransactionNotCommitted<T> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction is not comitted';
}

class ErrorDBTransactionRollbacked<T> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction is rollbacked';
}

class ErrorDBTransactionNotCommittedNorRollbacked<
  T,
> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction is not comitted nor rollbacked';
}

class ErrorDBTransactionConflict<T> extends ErrorDBTransaction<T> {
  static description = 'DBTransaction cannot commit due to conflicting writes';
}

class ErrorDBTransactionLockType<T> extends ErrorDBTransaction<T> {
  static description =
    'DBTransaction does not support upgrading or downgrading the lock type';
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
  ErrorDBIterator,
  ErrorDBIteratorDestroyed,
  ErrorDBIteratorBusy,
  ErrorDBTransaction,
  ErrorDBTransactionDestroyed,
  ErrorDBTransactionCommitted,
  ErrorDBTransactionNotCommitted,
  ErrorDBTransactionRollbacked,
  ErrorDBTransactionNotCommittedNorRollbacked,
  ErrorDBTransactionConflict,
  ErrorDBTransactionLockType,
};
