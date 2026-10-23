import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execute = promisify(execFile);
const identifier = (value) => {
  if (typeof value !== 'string' || !/^(disk|lease)_[A-Za-z0-9_-]{8,80}$/.test(value)) throw new Error('invalid cache resource id');
  return value;
};

/** Run on the QA/runner Mac only; never imports the production DB, host enrollment or image machinery. */
export function createMacCacheAdapter({ root, run = execute, platform = process.platform, guestLocal = false }) {
  if (platform !== 'darwin') throw new Error('APFS cache adapter requires macOS');
  if (!path.isAbsolute(root)) throw new Error('APFS cache root must be absolute');
  const diskPath = (diskId, generation) => {
    identifier(diskId);
    if (!Number.isSafeInteger(generation) || generation < 0) throw new Error('invalid cache generation');
    return path.join(root, `${diskId}.${generation}.sparsebundle`);
  };
  const leasePath = (leaseId) => path.join(root, identifier(leaseId));
  const command = (exe, args) => run(exe, args, { timeout: 120000, maxBuffer: 1024 * 1024, env: { PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: root } });
  // generation: the lease's generation when its stored image could not be fetched; an empty image stands in for it.
  const create = async ({ diskId, maxBytes, generation = 0 }) => {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 32 * 1024 ** 2) throw new Error('APFS capacity must be at least 32 MiB');
    const target = diskPath(diskId, generation);
    await fs.mkdir(root, { recursive: true, mode: 0o700 });
    await command('/usr/bin/hdiutil', ['create', '-size', `${Math.ceil(maxBytes / 512)}b`, '-fs', 'APFS', '-type', 'SPARSEBUNDLE', '-volname', 'XenoCI Cache', target]);
    return { path: target };
  };
  const attach = async ({ diskId, generation, leaseId, mode }) => {
    if (!['read', 'write'].includes(mode)) throw new Error('invalid cache lease mode');
    const owned = leasePath(leaseId), image = path.join(owned, 'cache.sparsebundle'), mount = path.join(owned, 'mount');
    await fs.mkdir(owned, { mode: 0o700 }); await fs.mkdir(mount);
    try {
      // cp -c uses clonefile on APFS; it must fail rather than silently becoming a Docker/full-copy implementation.
      await command('/bin/cp', ['-c', '-R', diskPath(diskId, generation), image]);
      await command('/usr/bin/hdiutil', ['attach', '-nobrowse', '-noautoopen', '-mountpoint', mount, ...(mode === 'read' ? ['-readonly'] : []), image]);
      return { diskId, generation, leaseId, mode, image, mount };
    } catch (error) {
      const { stdout } = await command('/sbin/mount', []);
      if (!stdout.includes(` on ${mount} (`)) await fs.rm(owned, { recursive: true, force: true });
      throw error;
    }
  };
  const detach = async (lease) => {
    if (lease.mount !== path.join(leasePath(lease.leaseId), 'mount')) throw new Error('foreign cache mount');
    await command('/usr/bin/hdiutil', ['detach', lease.mount]);
    // An unmount failure is never a cleanup receipt; keep the lease directory for recovery.
    const { stdout } = await command('/sbin/mount', []);
    if (stdout.includes(` on ${lease.mount} (`)) throw new Error('cache mount still active');
  };
  const measure = async (image) => {
    let bytes = 0; const digest = crypto.createHash('sha256');
    const walk = async (directory, relative = '') => {
      for (const name of (await fs.readdir(directory)).sort()) {
        const full = path.join(directory, name), rel = path.join(relative, name), stat = await fs.lstat(full);
        if (stat.isSymbolicLink()) throw new Error('cache image contains symlink');
        if (stat.isDirectory()) await walk(full, rel);
        else if (stat.isFile()) { const data = await fs.readFile(full); bytes += data.length; digest.update(`${rel}\0${data.length}\0`); digest.update(data); }
        else throw new Error('cache image contains special file');
      }
    };
    await walk(image); return { bytes, digest: digest.digest('hex') };
  };
  const publish = async (lease, { stopped, jobCompleted = false }) => {
    // Guest-local action mounts have no host/VM-shared block device. Its post hook ends writes before detach.
    if (lease.mode !== 'write' || (stopped !== true && !(guestLocal && jobCompleted === true))) throw new Error('writer must have a stopped guest or completed guest-local job before publishing');
    if (lease.image !== path.join(leasePath(lease.leaseId), 'cache.sparsebundle')) throw new Error('foreign cache image');
    await detach(lease);
    const measured = await measure(lease.image), target = diskPath(lease.diskId, lease.generation + 1);
    // Exclusive lease and control-plane CAS own authorization; the destination must never replace an existing generation.
    await fs.mkdir(`${target}.claim`);
    try { await fs.rename(lease.image, target); }
    finally { await fs.rmdir(`${target}.claim`); }
    await fs.rm(leasePath(lease.leaseId), { recursive: true });
    return { generation: lease.generation + 1, ...measured, path: target };
  };
  const release = async (lease) => { await detach(lease); await fs.rm(leasePath(lease.leaseId), { recursive: true }); return { unmounted: true }; };
  const remove = async ({ diskId, activeLeases }) => {
    identifier(diskId); if (activeLeases !== 0) throw new Error('active cache lease cannot be evicted');
    const { stdout } = await command('/sbin/mount', []);
    if (stdout.includes(root)) throw new Error('cache mount exists; refuse image deletion');
    for (const name of await fs.readdir(root)) if (name.startsWith(`${diskId}.`) && name.endsWith('.sparsebundle')) await fs.rm(path.join(root, name), { recursive: true });
    return { deleted: true, activeLeases: 0 };
  };
  return { create, attach, publish, release, remove, measure };
}
