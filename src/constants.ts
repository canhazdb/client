const constants : {[index: string]: number} = {
  STATUS_OK: 120,
  STATUS_BAD_REQUEST: 121,
  STATUS_NOT_FOUND: 122,
  STATUS_CREATED: 123,

  COMMAND: 40,
  STATUS: 41,

  INTERNAL: 2,

  COLLECTION_ID: 3,
  RESOURCE_ID: 4,

  DATA: 5,
  REPLICATED_NODES: 6,

  INFO: 7,
  COUNT: 8,
  GET: 9,
  POST: 10,
  PUT: 11,
  PATCH: 12,
  DELETE: 13,

  QUERY: 14,
  FIELDS: 15,
  LIMIT: 16,
  ORDER: 17,

  NOTIFY_ON: 18,
  NOTIFY_OFF: 19,

  ERROR: 26,

  LOCK: 20,
  UNLOCK: 21,
  LOCK_ID: 22,
  LOCK_KEYS: 27,
  LOCK_STRATEGY: 23,
  LOCK_STRATEGY_FAIL: 24,
  LOCK_STRATEGY_WAIT: 25
};

export const lookup : {[index: number]: string} = Object
  .keys(constants)
  .reduce((lookup, key) => {
    lookup[constants[key]] = key;
    return lookup;
  }, {});

export default constants;