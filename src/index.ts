import tcpocket from 'tcpocket';

import validateQueryOptions from './utils/validateQueryOptions.js';

import { CommandCodes as c } from './constants.js';

function checkKeys (allowedKeys, object) {
  return Object
    .keys(object)
    .filter(key => !allowedKeys.includes(key));
}

export interface CountOptions {
  query?: Object,
  limit?: Number,
  order?: String
}
export async function count (connection, collectionId, options: CountOptions = {}) {
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
      statusCode: c[response.command],
      getResponse: () => response
    });
  }

  return response.json()[c.DATA];
}

export interface GetAllOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  fields?: Array<String>,
}
export async function getAll (connection, collectionId, options: GetAllOptions = {}) {
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
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return response.json()[c.DATA];
}

export interface GetOneOptions {
  query?: Object,
  order?: String,
  fields?: Array<String>,
}
export async function getOne (connection, collectionId, options: GetOneOptions = {}) {
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
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return response.json()[c.DATA][0];
}


export interface PostOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String,
  fields?: Array<String>,
}
export async function post (connection, collectionId, document, options: PostOptions = {}) {
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
    const data = response.json()
    throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return response.json()[c.DATA];
}

export interface PutOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}
export async function put (connection, collectionId, document, options: PutOptions = {}) {
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
    const data = response.json()
    throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return {
    changes: response.json()[c.DATA]
  };
}

export interface PatchOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}
export async function patch (connection, collectionId, document, options: PatchOptions = {}) {
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
    const data = response.json()
    throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return {
    changes: response.json()[c.DATA]
  };
}

export interface DeleteOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}
export async function del (connection, collectionId, options: DeleteOptions = {}) {
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
    const data = response.json()
    throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return {
    changes: response.json()[c.DATA]
  };
}


export interface LockOptions {
  query?: Object,
  limit?: Number,
  order?: String,
  lockId?: String,
  lockStrategy?: String
}
export async function lock (connection, keys, options: LockOptions = {}) {
  if (!Array.isArray(keys)) {
    throw Object.assign(new Error('canhazdb error: keys must be array but got ' + keys.toString()));
  }

  if (options.query) {
    validateQueryOptions(options.query);
  }

  const response = await connection.send(c.LOCK, {
    [c.LOCK_KEYS]: keys
  });

  if (response.command !== c.STATUS_OK) {
    const data = response.json()
    throw Object.assign(new Error('canhazdb client: ' + data[c.ERROR]), {
      statusCode: c[response.command],
      request: options,
      getResponse: () => response
    });
  }

  return {
    lockId: response.json()[c.LOCK_ID]
  };
}


export interface ClientOptions {
  host: string,
  port: number,
  key?: string,
  cert?: string,
  ca?: string
}
/**
 * Create a connection to a canhazdb server
 * 
 * @name createClient
 */
export async function createClient (options: ClientOptions) {
  const connection = tcpocket.createClient(options);
  await connection.waitUntilConnected();

  function close () {
    connection.close();
  }

  return {
    connection,

    count: count.bind(null, connection),
    getAll: getAll.bind(null, connection),
    getOne: getOne.bind(null, connection),
    post: post.bind(null, connection),
    put: put.bind(null, connection),
    patch: patch.bind(null, connection),
    delete: del.bind(null, connection),
    lock: lock.bind(null, connection),

    close
  };
}

export default createClient;
