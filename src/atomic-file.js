import { fsyncSync } from 'node:fs';
import {
  closeSync,
  mkdirSync,
  openSync,
  renameSync,
  rmSync,
  writeSync
} from 'node:fs';
import { dirname } from 'node:path';

export function mkdirp(path) {
  mkdirSync(path, { recursive: true });
}

export function fsyncDirectory(path) {
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

/**
 * Atomically write a JSON file. A partial-file hook makes the write survive a
 * simulated crash before either the data file is complete or its rename is
 * committed by the containing directory.
 */
export function atomicWriteJson(filePath, value, hooks = {}) {
  mkdirp(dirname(filePath));
  const tmpPath = `${filePath}.tmp`;
  const payload = Buffer.from(JSON.stringify(value));

  const fd = openSync(tmpPath, 'w');
  try {
    if (payload.length > 0) {
      writeSync(fd, payload.subarray(0, 1));
      if (typeof hooks.afterFirstChunk === 'function') {
        hooks.afterFirstChunk({ filePath, tmpPath });
      }
      if (payload.length > 1) {
        writeSync(fd, payload.subarray(1));
      }
    }
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }

  renameSync(tmpPath, filePath);
  fsyncDirectory(dirname(filePath));

  if (typeof hooks.afterRename === 'function') {
    hooks.afterRename({ filePath });
  }
}

export function removeIfExists(path) {
  try {
    rmSync(path, { force: true });
    return true;
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    return false;
  }
}
