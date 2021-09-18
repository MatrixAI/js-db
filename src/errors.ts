import { CustomError } from 'ts-custom-error';

class ErrorDB extends CustomError {}

class ErrorDBRunning extends ErrorDB {}

class ErrorDBNotRunning extends ErrorDB {}

class ErrorDBDestroyed extends ErrorDB {}

class ErrorDBLevelPrefix extends ErrorDB {}

class ErrorDBDecrypt extends ErrorDB {}

class ErrorDBParse extends ErrorDB {}

class ErrorDBCommitted extends ErrorDB {}

class ErrorDBNotCommited extends ErrorDB {}

class ErrorDBRollbacked extends ErrorDB {}

export {
  ErrorDB,
  ErrorDBRunning,
  ErrorDBNotRunning,
  ErrorDBDestroyed,
  ErrorDBLevelPrefix,
  ErrorDBDecrypt,
  ErrorDBParse,
  ErrorDBCommitted,
  ErrorDBNotCommited,
  ErrorDBRollbacked,
};
