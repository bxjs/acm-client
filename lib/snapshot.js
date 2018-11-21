'use strict';

const fs = require('mz/fs');
const path = require('path');
const osenv = require('osenv');
const assert = require('assert');
const is = require('is-type-of');
const Base = require('sdk-base');
const { mkdirp, rimraf } = require('mz-modules');

const DEFAULT_OPITONS = {
  cacheDir: path.join(osenv.home(), '.node-diamond-client-cache'),
};

class Snapshot extends Base {
  constructor(options) {
    super(Object.assign({}, DEFAULT_OPITONS, options));
    this.ready(true);
  }

  get cacheDir() {
    return this.options.cacheDir;
  }

  * get(key) {
    const filepath = this._getSnapshotFile(key);
    try {
      const exists = yield fs.exists(filepath);
      if (exists) {
        return yield fs.readFile(filepath, 'utf8');
      }
    } catch (err) {
      err.name = 'SnapshotReadError';
      this.emit('error', err);
    }
    return null;
  }

  * save(key, value) {
    const filepath = this._getSnapshotFile(key);
    const dir = path.dirname(filepath);
    value = value || '';
    try {
      yield mkdirp(dir);
      yield fs.writeFile(filepath, value);
    } catch (err) {
      err.name = 'SnapshotWriteError';
      err.key = key;
      err.value = value;
      this.emit('error', err);
    }
  }

  * delete(key) {
    const filepath = this._getSnapshotFile(key);
    try {
      yield rimraf(filepath);
    } catch (err) {
      err.name = 'SnapshotDeleteError';
      err.key = key;
      this.emit('error', err);
    }
  }

  * batchSave(arr) {
    assert(is.array(arr), '[diamond#Snapshot] batchSave(arr) arr should be an Array.');
    yield arr.map(({ key, value }) => this.save(key, value));
  }

  _getSnapshotFile(key) {
    return path.join(this.cacheDir, 'snapshot', key);
  }
}

module.exports = Snapshot;
