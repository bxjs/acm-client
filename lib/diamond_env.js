'use strict';

const debug = require('debug')('diamond-client');
const co = require('co');
const path = require('path');
const assert = require('assert');
const Base = require('sdk-base');
const is = require('is-type-of');
const utils = require('./utils');
const gather = require('co-gather');
const Constants = require('./const');
const { sleep } = require('mz-modules');
const crypto = require('crypto');

const DEFAULT_OPTIONS = {
  serverPort: 8080,
  refreshInterval: 30 * 1000, // 30s
  requestTimeout: 5000,
  unit: Constants.CURRENT_UNIT,
  ssl: true,
};

class DiamondEnv extends Base {
  /**
   * Diamond Client.
   *
   * @param {Object} options
   *  - {Number} [refreshInterval] data refresh interval time, default is 30000 ms
   *  - {Number} [requestTimeout] diamond request timeout, default is 5000 ms
   *  - {String} [unit] unit name
   *  - {HttpClient} httpclient http request client
   *  - {Snapshot} snapshot snapshot instance
   * @constructor
   */
  constructor(options = {}) {
    assert(options.httpclient, '[DiamondEnv] options.httpclient is required');
    assert(options.snapshot, '[DiamondEnv] options.snapshot is required');
    assert(options.serverMgr, '[DiamondEnv] options.serverMgr is required');
    super(Object.assign({}, DEFAULT_OPTIONS, options));

    this._isClose = false;
    this._isLongPulling = false;
    this._subscriptions = new Map(); // key => { }
    this._currentServer = null;

    // 同一个key可能会被多次订阅，避免不必要的 `warning`
    this.setMaxListeners(100);
    this.ready(true);
  }

  get appName() {
    return this.options.appName;
  }

  get appKey() {
    return this.options.appKey;
  }

  get secretKey() {
    return this.options.secretKey;
  }

  get snapshot() {
    return this.options.snapshot;
  }

  get serverMgr() {
    return this.options.serverMgr;
  }

  get unit() {
    return this.options.unit;
  }

  /**
   * HTTP 请求客户端
   * @property {HttpClient} DiamondEnv#httpclient
   */
  get httpclient() {
    return this.options.httpclient;
  }

  close() {
    this._isClose = true;
    this.removeAllListeners();
  }

  /**
   * 更新 当前服务器
   */
  * _updateCurrentServer() {
    this._currentServer = yield this.serverMgr.getOne(this.unit);
    if (!this._currentServer) {
      const err = new Error('[DiamondEnv] Diamond server unavailable');
      err.name = 'DiamondServerUnavailableError';
      err.unit = this.unit;
      throw err;
    }
  }

  /**
   * 订阅
   * @param {Object} info
   *   - {String} dataId - id of the data you want to subscribe
   *   - {String} [group] - group name of the data
   * @param {Function} listener - listener
   * @return {DiamondEnv} self
   */
  subscribe(info, listener) {
    const { dataId, group } = info;
    const key = this._formatKey(info);
    this.on(key, listener);

    let item = this._subscriptions.get(key);
    if (!item) {
      item = {
        dataId,
        group,
        md5: null,
        content: null,
      };
      this._subscriptions.set(key, item);
      co(function* () {
        yield this._syncConfigs([ item ]);
        this._startLongPulling();
      }.bind(this)).catch(err => { this._error(err); });
    } else if (!is.nullOrUndefined(item.md5)) {
      process.nextTick(() => listener(item.content));
    }
    return this;
  }

  /**
   * 同步配置
   * @param {Array} list - 需要同步的配置列表
   * @return {void}
   */
  * _syncConfigs(list) {
    const tasks = list.map(({ dataId, group }) => this.getConfig(dataId, group));
    const results = yield gather(tasks, 5);
    for (let i = 0, len = results.length; i < len; i++) {
      const key = this._formatKey(list[i]);
      const item = this._subscriptions.get(key);
      const result = results[i];
      if (!item) {
        debug('item %s not exist', key); // maybe removed by user
        continue;
      }
      if (result.isError) {
        const err = new Error(`[DiamondEnv] getConfig failed for dataId: ${item.dataId}, group: ${item.group}, error: ${result.error}`);
        err.name = 'DiamondSyncConfigError';
        err.dataId = item.dataId;
        err.group = item.group;
        this._error(err);
        continue;
      }

      const content = result.value;
      const md5 = utils.getMD5String(content);
      // 防止应用启动时，并发请求，导致同一个 key 重复触发
      if (item.md5 !== md5) {
        item.md5 = md5;
        item.content = content;
        // 异步化，避免处理逻辑异常影响到 diamond 内部
        setImmediate(() => this.emit(key, content));
      }
    }
  }

  /**
   * 请求
   * @param {String} path - 请求 path
   * @param {Object} [options] - 参数
   * @return {String} value
   */
  * _request(path, options = {}) {
    // 默认为当前单元
    const unit = this.unit;
    const ts = String(Date.now());
    const { encode = false, method = 'GET', data, timeout = this.options.requestTimeout, headers = {} } = options;

    if (!this._currentServer) {
      yield this._updateCurrentServer();
    }

    let url = `http://${this._currentServer}:${8080}/diamond-server${path}`;

    if (this.options.ssl) {
      url = `https://${this._currentServer}:${443}/diamond-server${path}`;
    }

    debug('request unit: [%s] with url: %s', unit, url);
    let signStr = data.tenant;
    if (data.group && data.tenant) {
      signStr = data.tenant + '+' + data.group;
    } else if (data.group) {
      signStr = data.group;
    }

    const signature = crypto.createHmac('sha1', this.secretKey)
      .update(signStr + '+' + ts).digest()
      .toString('base64');
    // 携带统一的头部信息
    Object.assign(headers, {
      'Client-Version': Constants.VERSION,
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'Spas-AccessKey': this.options.accessKey,
      timeStamp: ts,
      exConfigInfo: 'true',
      'Spas-Signature': signature,
    });

    let requestData = data;
    if (encode) {
      requestData = utils.encodingParams(data);
    }
    try {
      const res = yield this.httpclient.request(url, {
        rejectUnauthorized: false,
        httpsAgent: false,
        method,
        data: requestData,
        dataType: 'text',
        headers,
        timeout,
        secureProtocol: 'TLSv1_2_method',
      });

      let err;
      const resData = res.data;
      debug('%s %s, got %s, body: %j', method, url, res.status, resData);
      switch (res.status) {
        case Constants.HTTP_OK:
          return resData;
        case Constants.HTTP_NOT_FOUND:
          return null;
        case Constants.HTTP_CONFLICT:
          err = new Error(`[DiamondEnv] Diamond server config being modified concurrently, data: ${JSON.stringify(data)}`);
          err.name = 'DiamondServerConflictError';
          throw err;
        default:
          err = new Error(`Diamond Server Error Status: ${res.status}, url: ${url}, request data: ${JSON.stringify(data)}, response data: ${resData && resData.toString()}`);
          err.name = 'DiamondServerResponseError';
          err.body = res.data;
          throw err;
      }
    } catch (err) {
      err.url = `${method} ${url}`;
      err.data = data;
      err.headers = headers;
      yield this._updateCurrentServer();
      throw err;
    }
  }

  /**
   * 开启长轮询
   * @return {void}
   * @private
   */
  _startLongPulling() {
    // 防止重入
    if (this._isLongPulling) {
      return;
    }
    this._isLongPulling = true;
    co(function* () {
      while (!this._isClose && this._subscriptions.size > 0) {
        try {
          yield this._checkServerConfigInfo();
        } catch (err) {
          err.name = 'DiamondLongPullingError';
          this._error(err);
          yield sleep(2000);
        }
      }
    }.bind(this)).then(() => {
      this._isLongPulling = false;
    }).catch(err => {
      this._isLongPulling = false;
      this._error(err);
    });
  }

  * _checkServerConfigInfo() {
    debug('start to check update config list');
    if (this._subscriptions.size === 0) {
      return;
    }

    const beginTime = Date.now();
    const tenant = this.options.namespace;
    const probeUpdate = [];
    for (const { dataId, group, md5 } of this._subscriptions.values()) {
      probeUpdate.push(dataId, Constants.WORD_SEPARATOR);
      probeUpdate.push(group, Constants.WORD_SEPARATOR);

      if (tenant) {
        probeUpdate.push(md5, Constants.WORD_SEPARATOR);
        probeUpdate.push(tenant, Constants.LINE_SEPARATOR);
      } else {
        probeUpdate.push(md5, Constants.LINE_SEPARATOR);
      }
    }
    const content = yield this._request('/config.co', {
      method: 'POST',
      data: {
        'Probe-Modify-Request': probeUpdate.join(''),
      },
      headers: {
        longPullingTimeout: '30000',
      },
      timeout: 40000, // 超时时间比longPullingTimeout稍大一点，避免主动超时异常
    });
    debug('long pulling takes %ds', (Date.now() - beginTime) / 1000);
    const updateList = this._parseUpdateDataIdResponse(content);
    if (updateList && updateList.length) {
      yield this._syncConfigs(updateList);
    }
  }

  // 解析 diamond 返回的 long pulling 结果
  _parseUpdateDataIdResponse(content) {
    const updateList = [];
    decodeURIComponent(content)
      .split(Constants.LINE_SEPARATOR)
      .forEach(dataIdAndGroup => {
        if (dataIdAndGroup) {
          const keyArr = dataIdAndGroup.split(Constants.WORD_SEPARATOR);
          if (keyArr.length >= 2) {
            const dataId = keyArr[0];
            const group = keyArr[1];
            updateList.push({
              dataId,
              group,
            });
          }
        }
      });
    return updateList;
  }

  /**
   * 退订
   * @param {Object} info
   *   - {String} dataId - id of the data you want to subscribe
   *   - {String} group - group name of the data
   * @param {Function} listener - listener
   * @return {DiamondEnv} self
   */
  unSubscribe(info, listener) {
    const key = this._formatKey(info);
    if (listener) {
      this.removeListener(key, listener);
    } else {
      this.removeAllListeners(key);
    }
    // 没有人订阅了，从长轮询里拿掉
    if (this.listeners(key).length === 0) {
      this._subscriptions.delete(key);
    }
    return this;
  }

  /**
   * 默认异常处理
   * @param {Error} err - 异常
   * @return {void}
   * @private
   */
  _error(err) {
    if (err) {
      setImmediate(() => this.emit('error', err));
    }
  }

  _formatKey(info) {
    return `${info.dataId}@${info.group}@${this.unit}`;
  }

  _getSnapshotKey(dataId, group, tenant) {
    tenant = tenant || this.options.namespace || 'default_tenant';
    return path.join('config', this.unit, tenant, group, dataId);
  }

  /**
   * 获取配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @return {String} value
   */
  * getConfig(dataId, group) {
    debug('calling getConfig, dataId: %s, group: %s', dataId, group);
    let content;
    const key = this._getSnapshotKey(dataId, group);
    try {
      content = yield this._request('/config.co', {
        data: {
          dataId,
          group,
          tenant: this.options.namespace,
        },
      });
    } catch (err) {
      const cache = yield this.snapshot.get(key);
      if (cache) {
        this._error(err);
        return cache;
      }
      throw err;
    }
    yield this.snapshot.save(key, content);
    return content;
  }

  /**
  * 查询租户下的所有的配置
  * @return {Array} config
  */
  * getAllConfigInfo() {
    const configInfoPage = yield this.getAllConfigInfoByTenantInner(1, 1);
    const total = configInfoPage.totalCount;
    const pageSize = 200;
    let configs = [];
    for (let i = 0; i * pageSize < total; i++) {
      const configInfo = yield this.getAllConfigInfoByTenantInner(i + 1, pageSize);
      configs = configs.concat(configInfo.pageItems);
    }
    return configs;
  }

  * getAllConfigInfoByTenantInner(pageNo, pageSize) {
    const ret = yield this._request('/basestone.do', {
      data: {
        pageNo,
        pageSize,
        method: 'getAllConfigInfoByTenant',
        tenant: this.options.namespace,
      },
    });
    return JSON.parse(ret);
  }


  /**
   * 发布配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {String} content - config value
   * @return {Boolean} success
   */
  * publishSingle(dataId, group, content) {
    yield this._request('/basestone.do?method=syncUpdateAll', {
      method: 'POST',
      encode: true,
      data: {
        dataId,
        group,
        content,
        tenant: this.options.namespace,
      },
    });
    return true;
  }

  /**
   * 删除配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @return {Boolean} success
   */
  * remove(dataId, group) {
    yield this._request('/datum.do?method=deleteAllDatums', {
      method: 'POST',
      data: {
        dataId,
        group,
        tenant: this.options.namespace,
      },
    });
    return true;
  }

  * publishAggr(dataId, group, datumId, content) {
    const appName = this.appName;
    yield this._request('/datum.do?method=addDatum', {
      method: 'POST',
      data: {
        dataId,
        group,
        datumId,
        content,
        appName,
        tenant: this.options.namespace,
      },
    });
    return true;
  }

  * removeAggr(dataId, group, datumId) {
    yield this._request('/datum.do?method=deleteDatum', {
      method: 'POST',
      data: {
        dataId,
        group,
        datumId,
        tenant: this.options.namespace,
      },
    });
    return true;
  }

  /**
   * 批量获取配置
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @return {Array} result
   */
  * batchGetConfig(dataIds, group) {
    const dataIdStr = dataIds.join(Constants.WORD_SEPARATOR);
    const content = yield this._request('/config.co?method=batchGetConfig', {
      method: 'POST',
      data: {
        dataIds: dataIdStr,
        group,
        tenant: this.options.namespace,
      },
    });

    try {
      /**
       * data 结构
       * [{status: 1, group: "test-group", dataId: 'test-dataId3', content: 'test-content'}]
       */
      const data = JSON.parse(content);
      const savedData = data.filter(d => d.status === 1).map(d => {
        const r = {};
        r.key = path.join(this._getSnapshotKey(d.dataId, d.group));
        r.value = d.content;
        return r;
      });
      yield this.snapshot.batchSave(savedData);
      return data;
    } catch (err) {
      err.name = 'DiamondBatchDeserializeError';
      err.data = content;
      throw err;
    }
  }

  /**
   * 批量查询
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @return {Object} result
   */
  * batchQuery(dataIds, group) {
    const dataIdStr = dataIds.join(Constants.WORD_SEPARATOR);
    const content = yield this._request('/admin.do?method=batchQuery', {
      method: 'POST',
      data: {
        dataIds: dataIdStr,
        group,
        tenant: this.options.namespace,
      },
    });

    try {
      return JSON.parse(content);
    } catch (err) {
      err.name = 'DiamondBatchDeserializeError';
      err.data = content;
      throw err;
    }
  }
}

module.exports = DiamondEnv;
