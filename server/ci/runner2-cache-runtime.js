import fs from 'node:fs/promises';
import { createReadStream, createWriteStream } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { once } from 'node:events';
import { pipeline } from 'node:stream/promises';
import http from 'node:http';
import https from 'node:https';
import { Readable } from 'node:stream';
import { createMacCacheAdapter } from './runner2-cache-mac.js';

/**
 * GET for the stored image through node:http/https rather than fetch (the gateway pin and sticky.ci.xeno are http://,
 * a direct control-plane blob is https://). Node 24's bundled undici aborts the whole process with an uncatchable
 * `assert(!this.paused)` when a large body is still being written to disk and the server closes the socket
 * (nodejs/undici#5360); the NAS node always sends
 * `Connection: close` (nas-sendfile.js), so a sticky restore crashed the action with an internal stack (prod run
 * 40081811049, sticky-grace 2026-10-24). node:http honours the runner's proxy under --use-env-proxy like fetch does.
 */
export function httpGetResponse(url, { headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = (new URL(url).protocol === 'https:' ? https : http).get(url, { headers }, (res) => {
      const out = new Headers();
      for (const [k, v] of Object.entries(res.headers)) if (v != null) out.set(k, Array.isArray(v) ? v.join(', ') : String(v));
      resolve(new Response([204, 304].includes(res.statusCode) ? (res.resume(), null) : Readable.toWeb(res), { status: res.statusCode, headers: out }));
    });
    req.once('error', reject);
  });
}

const fileName = /^(?:Info\.plist|Info\.bckup|token|bands\/[a-f0-9]+)$/;
const digestFile = async (name) => {
  const digest = crypto.createHash('sha256'); let bytes = 0;
  for await (const chunk of createReadStream(name)) { bytes += chunk.length; digest.update(chunk); }
  return { digest: digest.digest('hex'), bytes };
};

/** Private sparsebundle archive, streamed in fixed framing; never extracts arbitrary tar paths or links. */
export async function packMacCacheImage(image, target) {
  const output = createWriteStream(target, { flags: 'wx', mode: 0o600 });
  // A rejected bundle destroys the stream while its first write may still wait for the file to open; that buffered write
  // then fails after destroy. The stream always has a listener, so such a late error can never surface as uncaught.
  let streamError = null;
  output.on('error', (error) => { streamError ??= error; });
  const write = async (data) => { if (streamError) throw streamError; if (!output.write(data)) await once(output, 'drain'); };
  try {
    await write(Buffer.from('XCIAPFS1'));
    for (const name of (await fs.readdir(image)).sort()) {
      // 'lock': hdiutil's attach lock inside a mounted bundle (macOS 15+ guests, bs-cache 2026-10-20: every commit
      // failed on it). It carries no data and hdiutil recreates it on the next attach, so it is never archived.
      if (name === 'lock') continue;
      if (name === 'bands') {
        const stat = await fs.lstat(path.join(image, name));
        if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('unsafe sparsebundle bands');
      }
      const names = name === 'bands' ? (await fs.readdir(path.join(image, name))).sort().map((band) => `bands/${band}`) : [name];
      for (const relative of names) {
        if (!fileName.test(relative)) throw new Error(`unexpected sparsebundle file: ${JSON.stringify(relative.slice(0, 80))}`);
        const full = path.join(image, relative), stat = await fs.lstat(full);
        if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('unsafe sparsebundle file');
        const header = Buffer.alloc(12), nameBuffer = Buffer.from(relative);
        header.writeUInt32BE(nameBuffer.length); header.writeBigUInt64BE(BigInt(stat.size), 4);
        await write(header); await write(nameBuffer);
        let read = 0;
        for await (const chunk of createReadStream(full)) { read += chunk.length; await write(chunk); }
        if (read !== stat.size) throw new Error('sparsebundle changed while archiving');
      }
    }
    const finished = once(output, 'finish'); output.end(); await finished;
    return await digestFile(target);
  } catch (error) {
    // events.once would reject on that late 'error' and replace the real error; a bare close listener only waits.
    if (!output.closed) { const closed = new Promise((resolve) => output.once('close', resolve)); output.destroy(); await closed; }
    await fs.rm(target, { force: true }); throw error;
  }
}

export async function restoreMacCacheImage(archive, target, maxBytes) {
  const input = await fs.open(archive, 'r'), seen = new Set(); let offset = 0, total = 0;
  const read = async (size) => {
    const data = Buffer.alloc(size); let position = 0;
    while (position < size) { const n = await input.read(data, position, size - position, offset); if (!n.bytesRead) throw new Error('truncated sparsebundle archive'); position += n.bytesRead; offset += n.bytesRead; }
    return data;
  };
  await fs.mkdir(target, { mode: 0o700 });
  try {
    if (!(await read(8)).equals(Buffer.from('XCIAPFS1'))) throw new Error('invalid sparsebundle archive');
    const size = (await input.stat()).size;
    while (offset < size) {
      const header = await read(12), length = header.readUInt32BE(), bytes = Number(header.readBigUInt64BE(4));
      if (!Number.isSafeInteger(bytes) || length < 1 || length > 128 || total + bytes > maxBytes) throw new Error('oversized sparsebundle archive');
      const relative = (await read(length)).toString();
      if (!fileName.test(relative) || seen.has(relative)) throw new Error('unsafe sparsebundle archive path'); seen.add(relative); total += bytes;
      if (relative.startsWith('bands/')) await fs.mkdir(path.join(target, 'bands'), { recursive: true, mode: 0o700 });
      const output = await fs.open(path.join(target, relative), 'wx', 0o600);
      try { let left = bytes; while (left) { const chunk = await read(Math.min(left, 1024 * 1024)); await output.writeFile(chunk); left -= chunk.length; } }
      finally { await output.close(); }
    }
    if (!seen.has('Info.plist')) throw new Error('sparsebundle metadata missing');
    return { bytes: total };
  } catch (error) { await fs.rm(target, { recursive: true, force: true }); throw error; }
  finally { await input.close(); }
}

/** Concrete per-job Mac bridge. S00 runs it only inside the owned guest/runner runtime after S05 serialization. */
export function createMacStickyJobRuntime({ origin, orgPublicId, capability, jobPublicId, repo, ref, root,
  gateway = null, fetchImpl = fetch, downloadImpl = fetchImpl === fetch ? httpGetResponse : fetchImpl, guestLocal = false,
  adapter = createMacCacheAdapter({ root, guestLocal }), log = () => {} }) {
  const base = new URL(origin);
  if (base.protocol !== 'https:' || base.username || base.password || !path.isAbsolute(root)) throw new Error('invalid cache runtime configuration');
  const transport = gateway ? new URL(gateway) : base;
  if (gateway && transport.href !== 'http://artifacts.ci.xeno/') throw new Error('invalid internal cache gateway');
  const requestUrl = (url) => new URL(url.pathname + url.search, transport);
  // Through the office pin the node serves several consoles (production, dev): name the one this job belongs to so the
  // gateway relays there (artifact-gateway.js CACHE_ORIGIN_HEADER, sticky-devlease 2026-10-24). Direct calls need none.
  const viaHeaders = (url, headers = {}) => (gateway && url.origin === transport.origin ? { ...headers, 'x-xeno-cache-origin': base.origin } : headers);
  const endpoint = `/api/runner/v1/organizations/${encodeURIComponent(orgPublicId)}/cache/leases`;
  const api = async (route, body) => {
    const url = requestUrl(new URL(route, base));
    const response = await fetchImpl(url, { method: 'POST', redirect: 'error',
      headers: viaHeaders(url, { Authorization: `Bearer ${capability}`, 'Content-Type': 'application/json' }), body: JSON.stringify(body) });
    if (!response.ok) { const e = await response.json(); throw Object.assign(new Error(e.error?.code ?? 'cache_http_error'), { status: response.status }); }
    return (await response.json()).data;
  };
  // Blob URLs name the control plane (local store) or the office NAS pin http://sticky.ci.xeno (sticky-nas 2026-10-19),
  // which only the Mac proxy can reach; any other origin is refused.
  const signedUrl = (value) => {
    const url = new URL(value, base);
    if (url.username || url.password) throw new Error('foreign cache blob origin');
    if (url.href.startsWith('http://sticky.ci.xeno/objects/')) return url;
    if (url.origin !== base.origin) throw new Error('foreign cache blob origin');
    return requestUrl(url);
  };
  // kind: 'sticky' (stickydisk action) or 'checkout' (Git mirror of the checkout action, git-checkout 2026-10-21).
  const begin = async ({ key, maxBytes, mode = 'write', kind = 'sticky' }) => {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 32 * 1024 ** 2) throw new Error('invalid cache image capacity');
    // A disk in its unpaid grace refuses writers with 402; the job still gets its data read-only and commits nothing.
    // readOnly names why, so the action can tell the customer in one line (sticky-grace 2026-10-24).
    const request = (m) => api(endpoint, { kind, repo, key, ref, mode: m, job_id: jobPublicId });
    let readOnly = null;
    const lease = await request(mode).catch((error) => {
      if (mode !== 'write' || error.status !== 402) throw error;
      mode = 'read'; readOnly = error.message; return request('read');
    });
    const localId = `disk_${crypto.createHash('sha256').update(`${orgPublicId}\0${repo}\0${kind === 'sticky' ? '' : `${kind}\0`}${key}`).digest('hex')}`;
    await fs.mkdir(root, { recursive: true, mode: 0o700 });
    const archive = path.join(root, `${lease.lease_id}.download`), image = path.join(root, `${localId}.${lease.generation}.sparsebundle`);
    // The owner's trial terms: a missing or unreachable stored image never fails the build; the job starts an empty
    // disk and its commit writes the next generation (restored stays false).
    let restored = false, source = 'empty', downloadMs = 0, downloadBytes = 0;
    try {
      if (lease.generation > 0 && lease.read_url) {
        const started = Date.now();
        try {
          const readUrl = signedUrl(lease.read_url);
          const response = await downloadImpl(readUrl, { redirect: 'error', headers: viaHeaders(readUrl) });
          if (!response.ok || Number(response.headers.get('content-length')) > maxBytes * 2) { await response.body?.cancel(); throw new Error(`cache image download ${response.status}`); }
          await pipeline(response.body, createWriteStream(archive, { flags: 'wx', mode: 0o600 }));
          const receipt = await digestFile(archive);
          if (`"${receipt.digest}"` !== response.headers.get('etag') || (lease.digest && receipt.digest !== lease.digest)) throw new Error('cache image digest mismatch');
          await restoreMacCacheImage(archive, image, maxBytes);
          restored = true; source = response.headers.get('x-xeno-sticky') || 'server'; downloadBytes = receipt.bytes; downloadMs = Date.now() - started;
        } catch (error) {
          log(`sticky image unavailable, starting empty: ${error.message}`);
          await fs.rm(archive, { force: true }); await fs.rm(image, { recursive: true, force: true });
        }
      }
      if (!restored) await adapter.create({ diskId: localId, maxBytes, generation: lease.generation });
      const mounted = await adapter.attach({ diskId: localId, generation: lease.generation, leaseId: lease.lease_id, mode });
      return { lease, mounted, maxBytes, localId, restored, source, downloadMs, downloadBytes, readOnly };
    } catch (error) {
      await api(`${endpoint}/${encodeURIComponent(lease.lease_id)}/release`, {});
      // The adapter refuses removal if attach partly succeeded and a mount is still active.
      await adapter.remove({ diskId: localId, activeLeases: 0 });
      throw error;
    } finally { await fs.rm(archive, { force: true }); }
  };
  const finish = async (job, { succeeded, stopped, jobCompleted = false }) => {
    if (stopped !== true && !(guestLocal && jobCompleted === true)) throw new Error('guest must be stopped or guest-local job completed before sticky finalization');
    if (!succeeded || job.mounted.mode === 'read') { await adapter.release(job.mounted); return { committed: false }; }
    const published = await adapter.publish(job.mounted, { stopped, jobCompleted }), archive = path.join(root, `${job.lease.lease_id}.upload`);
    try {
      const receipt = await packMacCacheImage(published.path, archive);
      if (receipt.bytes > job.maxBytes) throw new Error('cache image exceeds reserved capacity');
      const writeUrl = signedUrl(job.lease.write_url);
      const response = await fetchImpl(writeUrl, { method: 'PUT', redirect: 'error', body: createReadStream(archive), duplex: 'half',
        headers: viaHeaders(writeUrl, { 'content-length': String(receipt.bytes) }) });
      if (!response.ok) throw new Error(`cache image upload failed ${response.status}`);
      return await api(`${endpoint}/${encodeURIComponent(job.lease.lease_id)}/commit`, { generation: job.lease.generation, ...receipt });
    } finally { await fs.rm(archive, { force: true }); }
  };
  const release = async (job) => api(`${endpoint}/${encodeURIComponent(job.lease.lease_id)}/release`, {});
  const cleanup = async (job, { activeLeases }) => adapter.remove({ diskId: job.localId, activeLeases });
  return { begin, finish, release, cleanup };
}
