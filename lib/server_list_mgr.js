'use strict';

const debug = require('debug')('diamond-client');
const co = require('co');
const path = require('path');
const assert = require('assert');
const Base = require('sdk-base');
const gather = require('co-gather');
const Constants = require('./const');
const { random } = require('utility');
const Snapshot = require('./snapshot');
const { sleep } = require('mz-modules');

const CURRENT_UNIT = Symbol('CURRENT_UNIT');
const DEFAULT_OPTIONS = {
  endpoint: 'acm.aliyun.com',
  refreshInterval: 30 * 1000, // 30s
};

class ServerListManager extends Base {
  /**
   * 服务地址列表管理器
   *
   * @param {Object} options
   *   - {HttpClient} httpclient - http 客户端
   *   - {Snapshot} [snapshot] - 快照对象
   *   - {String} nameServerAddr - 命名服务器地址 `hostname:port`
   * @constructor
   */
  constructor(options = {}) {
    assert(options.httpclient, '[diamond#ServerListManager] options.httpclient is required');
    if (!options.snapshot) {
      options.snapshot = new Snapshot(options);
    }
    if (options.endpoint) {
      const temp = options.endpoint.split(':');
      options.endpoint = temp[0] + ':' + (temp[1] || '8080');
    }
    super(Object.assign({}, DEFAULT_OPTIONS, options));

    this._isSync = false;
    this._isClosed = false;
    this._serverListCache = new Map(); // unit => { hosts: [ addr1, addr2 ], index }
    this._syncServers();
    this.ready(true);
  }

  get snapshot() {
    return this.options.snapshot;
  }

  get httpclient() {
    return this.options.httpclient;
  }

  get nameServerAddr() {
    // jmenv.tbsite.net:8080
    if (this.options.endpoint) {
      return this.options.endpoint;
    }
    return this.options.nameServerAddr;
  }

  get refreshInterval() {
    return this.options.refreshInterval;
  }

  /**
   * 关闭地址列表服务
   */
  close() {
    this._isClosed = true;
  }

  * _request(url, options) {
    const res = yield this.httpclient.request(url, options);
    const { status, data } = res;
    if (status !== 200) {
      const err = new Error(`[diamond#ServerListManager] request url: ${url} failed with statusCode: ${status}`);
      err.name = 'DiamondServerResponseError';
      err.url = url;
      err.params = options;
      err.body = res.data;
      throw err;
    }
    return data;
  }

  /*
   * 获取当前机器所在单元
   */
  * getCurrentUnit() {
    if (!this[CURRENT_UNIT]) {
      const url = `http://${this.nameServerAddr}/env`;
      const data = yield this._request(url, {
        timeout: this.options.requestTimeout,
        dataType: 'text',
      });
      const unit = data && data.trim();
      this[CURRENT_UNIT] = unit;
    }
    return this[CURRENT_UNIT];
  }

  /**
   * 获取某个单元的地址
   * @param {String} unit 单元名，默认为当前单元
   * @return {String} address
   */
  * getOne(unit = Constants.CURRENT_UNIT) {
    let serverData = this._serverListCache.get(unit);
    // 不存在则先尝试去更新一次
    if (!serverData) {
      serverData = yield this._fetchServerList(unit);
    }
    // 如果还没有，则返回 null
    if (!serverData || !serverData.hosts.length) {
      return null;
    }
    const choosed = serverData.hosts[serverData.index];
    serverData.index += 1;
    if (serverData.index >= serverData.hosts.length) {
      serverData.index = 0;
    }
    return choosed;
  }

  /**
   * 同步服务器列表
   * @return {void}
   */
  _syncServers() {
    if (this._isSync) {
      return;
    }
    co(function* () {
      this._isSync = true;
      while (!this._isClosed) {
        yield sleep(this.refreshInterval);
        const units = Array.from(this._serverListCache.keys());
        debug('syncServers for units: %j', units);
        const results = yield gather(units.map(unit => this._fetchServerList(unit)));
        for (let i = 0, len = results.length; i < len; i++) {
          if (results[i].isError) {
            const err = new Error(results[i].error);
            err.name = 'DiamondUpdateServersError';
            this.emit('error', err);
          }
        }
      }
      this._isSync = false;
    }.bind(this)).catch(err => { this.emit('error', err); });
  }

  // 获取某个单元的 diamond server 列表
  * _fetchServerList(unit = Constants.CURRENT_UNIT) {
    const key = this._formatKey(unit);
    const url = this._getRequestUrl(unit);
    let hosts;
    try {
      let data = yield this._request(url, {
        timeout: this.options.requestTimeout,
        dataType: 'text',
      });
      data = data || '';
      hosts = data.split('\n').map(host => host.trim()).filter(host => !!host);
      const length = hosts.length;
      debug('got %d hosts, the serverlist is: %j', length, hosts);
      if (!length) {
        const err = new Error('[diamond#ServerListManager] Diamond return empty hosts');
        err.name = 'DiamondServerHostEmptyError';
        err.unit = unit;
        throw err;
      }
      yield this.snapshot.save(key, JSON.stringify(hosts));
    } catch (err) {
      this.emit('error', err);
      const data = yield this.snapshot.get(key);
      if (data) {
        try {
          hosts = JSON.parse(data);
        } catch (err) {
          yield this.snapshot.delete(key);
          err.name = 'ServerListSnapShotJSONParseError';
          err.unit = unit;
          err.data = data;
          this.emit('error', err);
        }
      }
    }
    if (!hosts || !hosts.length) {
      // 这里主要是为了让后面定时同步可以执行
      this._serverListCache.set(unit, null);
      return null;
    }
    const serverData = {
      hosts,
      index: random(hosts.length),
    };
    this._serverListCache.set(unit, serverData);
    return serverData;
  }

  _formatKey(unit) {
    return path.join('server_list', unit);
  }

  // 获取请求 url
  _getRequestUrl(unit) {
    return unit === Constants.CURRENT_UNIT ?
      `http://${this.nameServerAddr}/diamond-server/diamond` :
      `http://${this.nameServerAddr}/diamond-server/diamond-unit-${unit}?nofix=1`;
  }

  /**
   * 获取单元列表
   * @return {Array} units
   */
  * fetchUnitLists() {
    const url = `http://${this.nameServerAddr}/diamond-server/unit-list?nofix=1`;
    let data = yield this._request(url, {
      timeout: this.options.requestTimeout,
      dataType: 'text',
    });
    data = data || '';
    const units = data.split('\n').map(unit => unit.trim()).filter(unit => unit);
    // 这个逻辑和 @彦林 确认去掉，java 下个版本也将去掉
    // http://gitlab.alibaba-inc.com/middleware/diamond/blob/master/diamond-client/src/main/java/com/taobao/diamond/client/impl/DiamondUnitSite.java#L126
    // if (units.indexOf('center') === -1) {
    //   units.push('center');
    // }
    return units;
  }
}

module.exports = ServerListManager;
