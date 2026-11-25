// 小欣 节点：VLESS over WebSocket（纯 Node 实现，不依赖 xray 二进制）
// 环境变量：UUID（必填）、WSPATH（默认 /）、PORT（由平台注入）
const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const UUID = String(process.env.UUID || '').trim().toLowerCase();
const WSPATH = String(process.env.WSPATH || '/').trim() || '/';
const PORT = parseInt(process.env.PORT || '8080', 10);

if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(UUID)) {
  console.error('UUID required (36-char uuid), got: ' + JSON.stringify(UUID));
  process.exit(1);
}
const UUID_BYTES = Buffer.from(UUID.replace(/-/g, ''), 'hex');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('ok');
});

const wss = new WebSocketServer({ server, path: WSPATH, perMessageDeflate: false });

// 解析 VLESS 头部，返回 null 表示数据还不够
function parseHeader(buf) {
  if (buf.length < 18) return null;
  const version = buf[0];
  const id = buf.subarray(1, 17);
  const optLen = buf[17];
  if (buf.length < 18 + optLen + 4) return null;
  let p = 18 + optLen;
  const cmd = buf[p++];
  const port = buf.readUInt16BE(p);
  p += 2;
  const atyp = buf[p++];
  let host;
  if (atyp === 1) {
    if (buf.length < p + 4) return null;
    host = `${buf[p]}.${buf[p + 1]}.${buf[p + 2]}.${buf[p + 3]}`;
    p += 4;
  } else if (atyp === 2) {
    if (buf.length < p + 1) return null;
    const len = buf[p++];
    if (buf.length < p + len) return null;
    host = buf.subarray(p, p + len).toString('utf8');
    p += len;
  } else if (atyp === 3) {
    if (buf.length < p + 16) return null;
    const parts = [];
    for (let i = 0; i < 16; i += 2) parts.push(buf.readUInt16BE(p + i).toString(16));
    host = parts.join(':');
    p += 16;
  } else {
    return { invalid: true };
  }
  return { version, id, cmd, host, port, rest: buf.subarray(p) };
}

const DEBUG = !!process.env.DEBUG;
const log = (...a) => { if (DEBUG) console.error('[vless]', ...a); };

wss.on('connection', (ws) => {
  ws.binaryType = 'nodebuffer';
  let pending = Buffer.alloc(0);
  let socket = null;
  let ready = false;
  let queued = [];

  const close = (why) => {
    log('close:', why);
    try { ws.close(); } catch {}
    try { socket && socket.destroy(); } catch {}
  };

  const connect = (host, port) => {
    log('dial', host + ':' + port);
    socket = net.connect({ host, port }, () => {
      ready = true;
      // VLESS 响应头：版本 0 + 附加信息长度 0
      ws.send(Buffer.from([0x00, 0x00]));
      for (const chunk of queued) socket.write(chunk);
      queued = [];
    });
    socket.setNoDelay(true);
    socket.on('data', (d) => {
      if (ws.readyState === ws.OPEN) ws.send(d);
    });
    socket.on('error', (e) => close('socket ' + e.message));
    socket.on('close', () => close('socket closed'));
  };

  ws.on('message', (data) => {
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!socket) {
      pending = Buffer.concat([pending, chunk]);
      const h = parseHeader(pending);
      if (!h) {
        log('wait header, got', pending.length, 'bytes');
        if (pending.length > 512) close('header too long');
        return;
      }
      if (h.invalid || h.version !== 0 || !h.id.equals(UUID_BYTES) || h.cmd !== 1) {
        close(`bad header v=${h.version} cmd=${h.cmd} uuid_ok=${h.id.equals(UUID_BYTES)}`);
        return;
      }
      const rest = h.rest;
      pending = Buffer.alloc(0);
      connect(h.host, h.port);
      if (rest.length) queued.push(rest);
      return;
    }
    if (ready) socket.write(chunk);
    else queued.push(chunk);
  });

  ws.on('close', close);
  ws.on('error', close);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`vless-ws listening on :${PORT}, path=${WSPATH}`);
});
