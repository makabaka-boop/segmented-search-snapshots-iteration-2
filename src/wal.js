import {
  closeSync,
  existsSync,
  ftruncateSync,
  fsyncSync,
  openSync,
  readFileSync,
  renameSync,
  writeSync
} from 'node:fs';
import { dirname } from 'node:path';
import { fsyncDirectory } from './atomic-file.js';

export class WriteAheadLog {
  constructor(path) {
    this.path = path;
    this.fd = openSync(path, 'a');
  }

  append(record) {
    const line = `${JSON.stringify(record)}\n`;
    const buffer = Buffer.from(line);
    let written = 0;
    while (written < buffer.length) {
      written += writeSync(this.fd, buffer, written);
    }
    fsyncSync(this.fd);
  }

  rewrite(records) {
    const tmpPath = `${this.path}.tmp`;
    const payload = Buffer.from(records.map((record) => `${JSON.stringify(record)}\n`).join(''));
    const fd = openSync(tmpPath, 'w');
    try {
      if (payload.length) writeSync(fd, payload);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(tmpPath, this.path);
    fsyncDirectory(dirname(this.path));
    // The rename swapped the file underneath us: this.fd still names the
    // previous, now unlinked inode, and appending to it would fsync bytes no
    // directory entry can ever reach. Rebind to the replacement before the
    // next append so acknowledged writes stay on the visible log.
    const nextFd = openSync(this.path, 'a');
    if (this.fd !== undefined) {
      closeSync(this.fd);
    }
    this.fd = nextFd;
  }

  close() {
    if (this.fd === undefined) return;
    try {
      fsyncSync(this.fd);
    } finally {
      closeSync(this.fd);
      this.fd = undefined;
    }
  }

  static readRecords(path, { repairTornTail = false } = {}) {
    if (!existsSync(path)) return [];
    const text = readFileSync(path, 'utf8');
    const completeText = text.endsWith('\n') ? text : text.slice(0, text.lastIndexOf('\n') + 1);

    if (repairTornTail && completeText.length !== text.length) {
      const fd = openSync(path, 'r+');
      try {
        ftruncateSync(fd, completeText.length);
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      fsyncDirectory(dirname(path));
    }

    const records = [];
    const lines = completeText.split('\n');
    if (lines.length && lines.at(-1) === '') lines.pop();
    for (const line of lines) {
      try {
        records.push(JSON.parse(line));
      } catch (error) {
        throw Object.assign(new Error('Corrupt WAL record'), {
          code: 'ERR_CORRUPT_WAL',
          cause: error
        });
      }
    }
    return records;
  }
}
