'use strict';

const path = require('path');
const osenv = require('osenv');
const assert = require('assert');
const Base = require('sdk-base');
const Constants = require('./const');
const Snapshot = require('./snapshot');
const DiamondEnv = require('./diamond_env');
const { checkParameters } = require('./utils');
const ServerListManager = require('./server_list_mgr');
const urllib = require('urllib');
// 默认参数
const DEFAULT_OPTIONS = {
  appKey: '',
  serverPort: 8080,
  requestTimeout: 6000,
  refreshInterval: 30000,
  cacheDir: path.join(osenv.home(), '.node-diamond-client-cache'),
  httpclient: urllib,
  ssl: true,
};

class DiamondClient extends Base {
  constructor(options = {}) {
    assert(options.endpoint, '[AcmClient] options.endpoint is required');
    assert(options.namespace, '[AcmClient] options.namespace is required');
    assert(options.accessKey, '[AcmClient] options.accessKey is required');
    assert(options.secretKey, '[AcmClient] options.secretKey is required');
    options = Object.assign({}, DEFAULT_OPTIONS, options);
    options.snapshot = new Snapshot(options);
    options.serverMgr = new ServerListManager(options);
    super(options);

    this._clients = new Map();
    this.snapshot.on('error', err => this._error(err));
    this.serverMgr.on('error', err => { this._error(err); });
    this.ready(true);
  }

  /**
   * HTTP 请求客户端
   * @property {Urllib} DiamondClient#urllib
   */
  get httpclient() {
    return this.options.httpclient || urllib;
  }

  get appName() {
    return this.options.appName;
  }

  get appKey() {
    return this.options.appKey;
  }

  get snapshot() {
    return this.options.snapshot;
  }

  get serverMgr() {
    return this.options.serverMgr;
  }

  /**
   * 获取当前机器所在机房
   * @return {String} currentUnit
   */
  * getCurrentUnit() {
    return yield this.serverMgr.getCurrentUnit();
  }

  /**
   * 获取所有单元信息
   * @return {Array} units
   */
  * getAllUnits() {
    return yield this.serverMgr.fetchUnitLists();
  }

  /**
   * 订阅
   * @param {Object} info
   *   - {String} dataId - id of the data you want to subscribe
   *   - {String} [group] - group name of the data
   *   - {String} [unit] - which unit you want to connect, default is current unit
   * @param {Function} listener - listener
   * @return {DiamondClient} self
   */
  subscribe(info, listener) {
    const { dataId, group } = info;
    checkParameters(dataId, group);
    const client = this._getClient(info);
    client.subscribe({ dataId, group }, listener);
    return this;
  }

  /**
   * 退订
   * @param {Object} info
   *   - {String} dataId - id of the data you want to subscribe
   *   - {String} [group] - group name of the data
   *   - {String} [unit] - which unit you want to connect, default is current unit
   * @param {Function} listener - listener
   * @return {DiamondClient} self
   */
  unSubscribe(info, listener) {
    const { dataId, group } = info;
    checkParameters(dataId, group);
    const client = this._getClient(info);
    client.unSubscribe({ dataId, group }, listener);
    return this;
  }

  /**
   * 获取配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {String} value
   */
  * getConfig(dataId, group, options) {
    checkParameters(dataId, group);
    const client = this._getClient(options);
    return yield client.getConfig(dataId, group);
  }

  /**
  * 查询租户下的所有的配置
  * @return {Array} config
  */
  * getAllConfigInfo() {
    const client = this._getClient();
    return yield client.getAllConfigInfo();
  }


  /**
   * 发布配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {String} content - config value
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Boolean} success
   */
  * publishSingle(dataId, group, content, options) {
    checkParameters(dataId, group);
    const client = this._getClient(options);
    return yield client.publishSingle(dataId, group, content);
  }

  /**
   * 删除配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Boolean} success
   */
  * remove(dataId, group, options) {
    checkParameters(dataId, group);
    const client = this._getClient(options);
    return yield client.remove(dataId, group);
  }

  /**
   * 批量获取配置
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Array} result
   */
  * batchGetConfig(dataIds, group, options) {
    checkParameters(dataIds, group);
    const client = this._getClient(options);
    return yield client.batchGetConfig(dataIds, group);
  }

  /**
   * 批量查询
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Object} result
   */
  * batchQuery(dataIds, group, options) {
    checkParameters(dataIds, group);
    const client = this._getClient(options);
    return yield client.batchQuery(dataIds, group);
  }

  /**
   * 将配置发布到所有单元
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {String} content - config value
   * @return {Boolean} success
   */
  * publishToAllUnit(dataId, group, content) {
    checkParameters(dataId, group);
    const units = yield this.getAllUnits();
    yield units.map(unit => this._getClient({ unit }).publishSingle(dataId, group, content));
    return true;
  }

  /**
   * 将配置从所有单元中删除
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @return {Boolean} success
   */
  * removeToAllUnit(dataId, group) {
    checkParameters(dataId, group);
    const units = yield this.getAllUnits();
    yield units.map(unit => this._getClient({ unit }).remove(dataId, group));
    return true;
  }

  * publishAggr(dataId, group, datumId, content, options) {
    checkParameters(dataId, group, datumId);
    const client = this._getClient(options);
    return yield client.publishAggr(dataId, group, datumId, content);
  }

  * removeAggr(dataId, group, datumId, options) {
    checkParameters(dataId, group, datumId);
    const client = this._getClient(options);
    return yield client.removeAggr(dataId, group, datumId);
  }

  close() {
    this.serverMgr.close();
    for (const client of this._clients.values()) {
      client.close();
    }
    this._clients.clear();
  }

  _getClient(options = {}) {
    const { unit = Constants.CURRENT_UNIT } = options;
    let client = this._clients.get(unit);
    if (!client) {
      client = new DiamondEnv(Object.assign({}, this.options, { unit }));
      client.on('error', err => { this._error(err); });
      this._clients.set(unit, client);
    }
    return client;
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
}

module.exports = DiamondClient;
