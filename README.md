# canhazdb-client
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/canhazdb/client)
[![GitHub package.json version](https://img.shields.io/github/package-json/v/canhazdb/client)](https://github.com/canhazdb/client/blob/master/package.json)
[![GitHub](https://img.shields.io/github/license/canhazdb/client)](https://github.com/canhazdb/client/blob/master/LICENSE)
[![js-semistandard-style](https://img.shields.io/badge/code%20style-semistandard-brightgreen.svg)](https://github.com/standard/semistandard)

A client to simplify making rest api calls by using database like functions.

## Getting Started
You should [create a server](https://github.com/canhazdb/server#server-via-the-cli) before
trying to use the client.

### Client

#### Connecting
```javascript
const client = require('canhazdb-client');

const tls = {
  key: fs.readFileSync('./certs/localhost.privkey.pem'),
  cert: fs.readFileSync('./certs/localhost.cert.pem'),
  ca: [ fs.readFileSync('./certs/ca.cert.pem') ],
  requestCert: true /* this denys any cert not signed with our ca above */
};
const client = createClient('https://localhost:8063', { tls });
```

#### Making requests
```javascript
const document = await client.post('tests', { a: 1 });
const changed = await client.put('tests', { id: document.id }, { query: { b: 2 } });
const changedDocument = await client.getOne('tests', { query: { id: document.id } });
```

#### Using events
```javascript
// Capture an event based on regex
// client.on('.*:/tests/.*', ...)
// client.on('.*:/tests/uuid-uuid-uuid-uuid', ...)
// client.on('POST:/tests', ...)
// client.on('DELETE:/tests/.*', ...)
// client.on('(PUT|PATCH):/tests/uuid-uuid-uuid-uuid', ...)

client.on('POST:/tests/.*', (path, collectionId, resourceId, pattern) => {
  console.log(path) // === 'POST:/tests/uuid-uuid-uuid-uuid'
  console.log(collectionId) // === 'tests'
  console.log(resourceId) // === 'uuid-uuid-uuid-uuid'
  console.log(pattern) // === 'POST:/tests/.*'
})

console.log( {
  document, /* { a: 1 } */
  changed, /* { changes: 1 } */
  changedDocument, /* { b: 2 } */
})
```

### Examples
<details>
<summary>1. Get item by id</summary>
```javascript
client.get('tests', { 
  query: {
    id: 'example-uuid-paramater'
  }
});
```
</details>

<details>
<summary>2. Get document count in a collection</summary>
```javascript
client.count('tests', {
  query: {
    firstName: 'Joe'
  }
});
```
</details>

<details>
<summary>3. Get items in a collection</summary>
**Client:**
```javascript
client.get('tests', {
  query: {
    firstName: 'Joe'
  },
  limit: 10,
  order: 'desc(firstName)'
});
```
</details>

<details>
<summary>4. Create a new document in a collection</summary>
```javascript
client.post('tests', {
  firstName: 'Joe'
});
```
</details>

<details>
<summary>5. Replace a document by id</summary>
```javascript
client.put('tests', {
  firstName: 'Joe'
});
```
</details>

<details>
<summary>6. Replace multiple documents by query</summary>
```javascript
client.put('tests', {
    firstName: 'Zoe',
    location: 'GB',
    timezone: 'GMT'
}, {
  query: {
    location: 'GB'
  }
});
```
</details>

<details>
<summary>7. Partially update multiple documents by id</summary>
```javascript
client.patch('tests', {
    timezone: 'GMT'
}, {
  query: {
    location: 'GB'
  }
});
```
</details>

<details>
<summary>8. Partially update multiple documents by query</summary>
```javascript
client.patch('tests', {
    timezone: 'GMT'
}, {
  query: {
    location: 'GB'
  }
});
```
</details>

<details>
<summary>9. Delete a document by id</summary>
```javascript
client.delete('tests', {
  query: {
    id: 'example-uuid-paramater'
  }
});
```

</details>

<details>
<summary>10. Delete multiple documents by query</summary>
```javascript
client.delete('tests', {
  query: {
    location: 'GB'
  }
});
```
</details>

<details>
<summary>11. Lock a collection/document/field combination</summary>
```javascript
const lockId = await client.lock('users');
```
</details>

<details>
<summary>12. Release a lock</summary>
```javascript
const lockId = await client.lock(['users']);
const newDocument = await client.post('users', {
  name: 'mark'
}, {
  lockId,
  lockStrategy: 'wait' // optional: can be 'fail' or 'wait'. default is 'wait'.
});
await client.unlock(lockId);
```
</details>

## License
This project is licensed under the terms of the AGPL-3.0 license.
