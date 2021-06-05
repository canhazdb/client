const { promisify } = require('util');

const https = require('https');
const querystring = require('querystring');
const finalStream = require('final-stream');
const WebSocket = require('ws');
const ReconnectingWebSocket = require('reconnecting-websocket');

const validateQueryOptions = require('./utils/validateQueryOptions');

const {
  COLLECTION_ID,
  DOCUMENT,
  DOCUMENTS,
  STATUS,
  QUERY,
  LIMIT,
  DATA,
  FIELDS,
  ORDER
} = require('./constants');

function createWebSocketClass (options) {
  return class extends WebSocket {
    constructor (url, protocols) {
      super(url, protocols, options);
    }
  };
}

function checkKeys (allowedKeys, object) {
  return Object
    .keys(object)
    .filter(key => !allowedKeys.includes(key));
}

function client (rootUrl, clientOptions) {
  let lastAcceptId = 0;

  const httpsAgent = clientOptions && clientOptions.tls && new https.Agent(clientOptions.tls);

  function count (collectionId, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const query = querystring.encode({
      ...options,
      count: true,
      query: options.query && JSON.stringify(options.query)
    });

    const url = `${rootUrl}/${collectionId}?${query}`;
    https.request(url, { agent: httpsAgent }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);
      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, data);
    }).end();
  }

  function getAll (collectionId, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query', 'fields', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (!rws) {
      if (options.query) {
        validateQueryOptions(options.query);
      }
  
      const query = querystring.encode({
        ...options,
        query: options.query && JSON.stringify(options.query),
        fields: options.fields && JSON.stringify(options.fields),
        order: options.order && JSON.stringify(options.order)
      });
  
      const url = `${rootUrl}/${collectionId}?${query}`;
      https.request(url, { agent: httpsAgent }, async function (response) {
        const data = await finalStream(response).then(JSON.parse);
        if (response.statusCode >= 400) {
          callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
          return;
        }
  
        callback(null, data);
      }).end();
    }

    lastAcceptId = lastAcceptId + 1;
    onOffAccepts.push([lastAcceptId, (error, result) => {
      if (result[STATUS] !== 200) {
        const error = Object.assign(new Error('canhazdb error'), {
          data: result[DATA]
        });
        callback(error);
        return
      }
      callback(null, result[DOCUMENTS]);
    }]);
    rws.send(JSON.stringify([lastAcceptId, 'GET', {
      [COLLECTION_ID]: collectionId,
      [QUERY]: options.query,
      [LIMIT]: options.limit,
      [FIELDS]: options.fields,
      [ORDER]: options.order
    }]));
  }

  function getOne (collectionId, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query', 'fields', 'order'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (!rws) {
      options.limit = 1;

      if (options.query) {
        validateQueryOptions(options.query);
      }

      const query = querystring.encode({
        ...options,
        query: options.query && JSON.stringify(options.query),
        fields: options.fields && JSON.stringify(options.fields),
        order: options.order && JSON.stringify(options.order)
      });

      const url = `${rootUrl}/${collectionId}?${query}`;
      https.request(url, { agent: httpsAgent }, async function (response) {
        const data = await finalStream(response).then(JSON.parse);

        if (response.statusCode >= 400) {
          callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
          return;
        }

        callback(null, data[0]);
      }).end();
    }

    lastAcceptId = lastAcceptId + 1;
    onOffAccepts.push([lastAcceptId, (error, result) => {
      if (result[STATUS] !== 200) {
        const error = Object.assign(new Error('canhazdb error'), {
          data: result[DATA]
        });
        callback(error);
        return
      }
      callback(null, result[DOCUMENTS][0]);
    }]);
    rws.send(JSON.stringify([lastAcceptId, 'GET', {
      [COLLECTION_ID]: collectionId,
      [QUERY]: options.query,
      [LIMIT]: 1,
      [FIELDS]: options.fields,
      [ORDER]: options.order
    }]));
  }

  function post (collectionId, document, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['lockId', 'lockStrategy'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (!rws) {
      const url = `${rootUrl}/${collectionId}`;
      https.request(url, {
        agent: httpsAgent,
        method: 'POST'
      }, async function (response) {
        const data = await finalStream(response).then(JSON.parse);

        if (response.statusCode >= 400) {
          callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
          return;
        }

        callback(null, data);
      }).end(JSON.stringify(document));

      return;
    }

    lastAcceptId = lastAcceptId + 1;
    onOffAccepts.push([lastAcceptId, (error, result) => {
      callback(error, result[DOCUMENT]);
    }]);
    rws.send(JSON.stringify([lastAcceptId, 'POST', {
      [COLLECTION_ID]: collectionId,
      [DOCUMENT]: document
    }]));
  }

  function put (collectionId, newDocument, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query', 'lockId', 'lockStrategy'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const query = querystring.encode({
      ...options,
      query: options.query && JSON.stringify(options.query)
    });

    const url = `${rootUrl}/${collectionId}?${query}`;

    https.request(url, {
      agent: httpsAgent,
      method: 'PUT'
    }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);

      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, data);
    }).end(JSON.stringify(newDocument));
  }

  function patch (collectionId, newDocument, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query', 'lockId', 'lockStrategy'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const query = querystring.encode({
      ...options,
      query: options.query && JSON.stringify(options.query)
    });

    const url = `${rootUrl}/${collectionId}?${query}`;

    https.request(url, {
      agent: httpsAgent,
      method: 'PATCH'
    }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);

      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, data);
    }).end(JSON.stringify(newDocument));
  }

  function del (collectionId, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }

    const unknownKeys = checkKeys(['query', 'lockId', 'lockStrategy'], options);
    if (unknownKeys.length > 0) {
      callback(Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(','))));
      return;
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const query = querystring.encode({
      ...options,
      query: options.query && JSON.stringify(options.query)
    });

    const url = `${rootUrl}/${collectionId}?${query}`;
    https.request(url, {
      agent: httpsAgent,
      method: 'DELETE'
    }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);

      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, data);
    }).end();
  }

  function lock (keys, callback) {
    if (!Array.isArray(keys)) {
      callback(Object.assign(new Error('canhazdb error: keys must be array but got ' + keys.toString())));
      return;
    }

    const url = `${rootUrl}/_/locks`;
    https.request(url, {
      agent: httpsAgent,
      method: 'POST'
    }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);

      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, data.id);
    }).end(JSON.stringify(keys));
  }

  function unlock (lockId, callback) {
    const url = `${rootUrl}/_/locks/${lockId}`;
    https.request(url, {
      agent: httpsAgent,
      method: 'DELETE'
    }, async function (response) {
      const data = await finalStream(response).then(JSON.parse);

      if (response.statusCode >= 400) {
        callback(Object.assign(new Error('canhazdb error'), { data, statusCode: response.statusCode }));
        return;
      }

      callback(null, true);
    }).end();
  }

  const handlers = [];
  let wsUrl;
  if (rootUrl.startsWith('https://')) {
    wsUrl = rootUrl.replace('https://', 'wss://');
  } else {
    wsUrl = rootUrl.replace('http://', 'ws://');
  }

  const wsOptions = {
    WebSocket: createWebSocketClass({
      ...(clientOptions && clientOptions.tls)
    }),
    connectionTimeout: 1000,
    maxRetries: 10
  };

  const rws = clientOptions.disableNotify ? null : new ReconnectingWebSocket(wsUrl, [], wsOptions);
  const onOffAccepts = [];

  async function on (path, handler) {
    if (!rws) {
      throw new Error('notify was disable for this client instance');
    }

    lastAcceptId = lastAcceptId + 1;

    const existingHandler = handlers.find(item => item[0] === path);
    handlers.push([path, handler]);

    if (!existingHandler) {
      const promise = new Promise(resolve => {
        onOffAccepts.push([lastAcceptId, resolve]);
      });
      rws.send(JSON.stringify([lastAcceptId, 'NOTIFY', path]));
      return promise;
    }

    return true;
  }

  function off (path, handler) {
    if (!rws) {
      throw new Error('notify was disable for this client instance');
    }

    lastAcceptId = lastAcceptId + 1;
    const promise = new Promise(resolve => {
      onOffAccepts.push([lastAcceptId, resolve]);
    });

    rws.send(JSON.stringify([lastAcceptId, 'UNNOTIFY', path]));
    const index = handlers.findIndex(item => item[0] === path && item[1] === handler);

    if (index === -1) {
      return;
    }
    handlers.splice(index, 1);

    return promise;
  }

  rws && rws.addEventListener('message', (event) => {
    const rawData = JSON.parse(event.data);
    const [type, data] = rawData;
    if (type === 'A') {
      const accepterIndex = onOffAccepts.findIndex(item => item[0] === data);
      if (accepterIndex === -1) {
        return
      }
      const accepter = onOffAccepts[accepterIndex];
      accepter && accepter[1] && accepter[1](null, rawData[2]);
      onOffAccepts.splice(accepterIndex, 1);
      return;
    }
    handlers.forEach(item => {
      if (item[0] === data[3]) {
        item[1](...data);
      }
    });
  });

  function close () {
    rws && rws.close();
  }

  return {
    count: promisify(count),
    getAll: promisify(getAll),
    getOne: promisify(getOne),
    put: promisify(put),
    patch: promisify(patch),
    post: promisify(post),
    delete: promisify(del),
    lock: promisify(lock),
    unlock: promisify(unlock),

    on,
    off,

    close
  };
}

module.exports = client;
