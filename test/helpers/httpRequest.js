import fs from 'fs';
import https from 'https';
import axios from 'axios';

const httpsAgent = new https.Agent({
  key: fs.readFileSync('./certs/localhost.privkey.pem'),
  cert: fs.readFileSync('./certs/localhost.cert.pem'),
  ca: [fs.readFileSync('./certs/ca.cert.pem')]
});

export default axios
  .create({ httpsAgent, validateStatus: () => true });
