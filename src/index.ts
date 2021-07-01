import tcpocket from 'tcpocket';

import validateQueryOptions from './utils/validateQueryOptions.js';

import c, { lookup } from './constants.js';

function checkKeys (allowedKeys, object) {
  return Object
    .keys(object)
    .filter(key => !allowedKeys.includes(key));
}

interface CountOptions {
  query?: Object,
  limit?: Number,
  order?: String
}

interface GetAllOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  fields?: Array<String>,
}

interface OneOptions {
  query?: Object,
  order?: String,
  fields?: Array<String>,
}

interface PostOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String,
  fields?: Array<String>,
}

interface DeleteOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}

interface PatchOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}

interface PutOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}

async function client (clientOptions) {
  const connection = tcpocket.createClient(clientOptions);
  await connection.waitUntilConnected();

  async function count (collectionId, options: CountOptions = {}) {
    const unknownKeys = checkKeys(['query', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.COUNT, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: options.limit,
      [c.ORDER]: options.order
    });

    if (response.command !== c.STATUS_OK) {
      const data = response.json()
      throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
        statusCode: lookup[response.command],
        getResponse: () => response
      });
    }

    return response.json()[c.DATA];
  }

  async function getAll (collectionId, options: GetAllOptions = {}) {
    const unknownKeys = checkKeys(['query', 'fields', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.GET, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: options.limit,
      [c.FIELDS]: options.fields,
      [c.ORDER]: options.order
    });

    if (response.command !== c.STATUS_OK) {
      const data = response.json()

      throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
        statusCode: lookup[response.command],
        request: options,
        getResponse: () => response
      });
    }

    return response.json()[c.DATA];
  }

  async function getOne (collectionId, options: OneOptions = {}) {
    const unknownKeys = checkKeys(['query', 'fields', 'order'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.GET, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: 1,
      [c.FIELDS]: options.fields,
      [c.ORDER]: options.order
    });

    if (response.command !== c.STATUS_OK) {
      const data = response.json()
      throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
        statusCode: lookup[response.command],
        request: options,
        getResponse: () => response
      });
    }

    return response.json()[c.DATA][0];
  }

  async function post (collectionId, document, options: PostOptions = {}) {
    const unknownKeys = checkKeys(['lockId', 'lockStrategy'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.POST, {
      [c.COLLECTION_ID]: collectionId,
      [c.DATA]: document,
      [c.LOCK_ID]: options.lockId,
      [c.LOCK_STRATEGY]: options.lockStrategy
    });

    if (response.command !== c.STATUS_CREATED) {
      throw Object.assign(new Error('canhazdb error'), {
        statusCode: lookup[response.command], response
      });
    }

    return response.json()[c.DATA];
  }

  async function put (collectionId, document, options: PutOptions = {}) {
    const unknownKeys = checkKeys(['query', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.PUT, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: options.limit,
      [c.ORDER]: options.order,
      [c.DATA]: document,
      [c.LOCK_ID]: options.lockId,
      [c.LOCK_STRATEGY]: options.lockStrategy
    });

    if (response.command !== c.STATUS_OK) {
      throw Object.assign(new Error('canhazdb error'), {
        statusCode: lookup[response.command], response
      });
    }

    return {
      changes: response.json()[c.DATA]
    };
  }

  async function patch (collectionId, document, options: PatchOptions = {}) {
    const unknownKeys = checkKeys(['query', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.PATCH, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: options.limit,
      [c.ORDER]: options.order,
      [c.DATA]: document,
      [c.LOCK_ID]: options.lockId,
      [c.LOCK_STRATEGY]: options.lockStrategy
    });

    if (response.command !== c.STATUS_OK) {
      throw Object.assign(new Error('canhazdb error'), {
        statusCode: lookup[response.command], response
      });
    }

    return {
      changes: response.json()[c.DATA]
    };
  }

  async function del (collectionId, options: DeleteOptions = {}) {
    const unknownKeys = checkKeys(['query', 'order', 'limit'], options);
    if (unknownKeys.length > 0) {
      throw Object.assign(new Error('canhazdb error: unknown keys ' + unknownKeys.join(',')));
    }

    if (options.query) {
      validateQueryOptions(options.query);
    }

    const response = await connection.send(c.DELETE, {
      [c.COLLECTION_ID]: collectionId,
      [c.QUERY]: options.query,
      [c.LIMIT]: options.limit,
      [c.ORDER]: options.order,
      [c.LOCK_ID]: options.lockId,
      [c.LOCK_STRATEGY]: options.lockStrategy
    });

    if (response.command !== c.STATUS_OK) {
      throw Object.assign(new Error('canhazdb error'), {
        statusCode: lookup[response.command], response
      });
    }

    return {
      changes: response.json()[c.DATA]
    };
  }

  function close () {
    connection.close();
  }

  return {
    count,
    getAll,
    getOne,
    post,
    put,
    patch,
    delete: del,

    close
  };
}

export default client;
