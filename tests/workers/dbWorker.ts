import type { DBWorkerModule } from './dbWorkerModule';
import { expose } from 'threads/worker';
import dbWorker from './dbWorkerModule';

expose(dbWorker);

export type { DBWorkerModule };
