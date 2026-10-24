import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createMacStickyJobRuntime } from '../../server/ci/runner2-cache-runtime.js';

const env = process.env;
// Same wording as server/ci/runner2-sticky-disks.js STICKY_ROOM_MESSAGES (the action runs on the guest without the server tree).
const ROOM = { sticky_capacity_full: '지금은 스티키 디스크 자리가 없어요. 이번 잡은 스티키 디스크 없이 진행돼요',
  sticky_org_full: '조직의 스티키 디스크가 최대 용량에 이르렀어요. 이번 잡은 스티키 디스크 없이 진행돼요' };
// A disk in its unpaid grace (402 insufficient_credit on the write lease): the job still mounts it read-only (sticky-grace 2026-10-24).
const READ_ONLY = { insufficient_credit: '잔액이 없어 스티키 디스크가 읽기 전용입니다, 충전하면 다시 쓸 수 있어요' };
// Native fetch does not use the runner's mandatory egress proxy unless Node opts in at startup.
if ((env.HTTPS_PROXY || env.https_proxy) && !process.execArgv.includes('--use-env-proxy')) {
  const child = spawn(process.execPath, ['--use-env-proxy', ...process.execArgv, ...process.argv.slice(1)], { env, stdio: 'inherit' });
  const [code] = await once(child, 'close');
  process.exit(code ?? 1);
}
try {
  if (env.STATE_XENOCI_STICKY_STARTED && !env.STATE_XENOCI_STICKY_STATE) process.exit(0);
  const config = JSON.parse(env.XENOCI_STICKY_CONFIG ?? 'null');
  if (!config || !/^https:\/\/[^/]+$/.test(config.origin ?? '') || !env.XENOCI_CACHE_JOB_CAPABILITY) throw new Error('Sticky disks require a XenoCI runner');
  // The runner's cache probe (runner2-github-jobs.js cacheShell) chose the office pin when the origin was not reachable
  // directly: the disk control calls then go through the same pin.
  if (!config.gateway) {
    const verdict = await fs.readFile(path.join(os.homedir(), '.xenoci-results-url'), 'utf8').catch(() => '');
    if (verdict.trim() === 'http://artifacts.ci.xeno/') config.gateway = 'http://artifacts.ci.xeno/';
  }
  console.log(`::add-mask::${env.XENOCI_CACHE_JOB_CAPABILITY}`);
  const stateFile = env.STATE_XENOCI_STICKY_STATE;
  if (stateFile) {
    const { job, mountPath, config: savedConfig } = JSON.parse(await fs.readFile(stateFile, 'utf8'));
    const runtime = createMacStickyJobRuntime({ ...savedConfig, capability: env.XENOCI_CACHE_JOB_CAPABILITY });
    // This image is mounted inside the ephemeral guest, not attached to a live guest from its host.
    // The runner invokes the post hook after workflow steps stop writing; detach must succeed before upload.
    // Free trial terms (sticky-nas 2026-10-19): a failed save is a warning, the next build starts from the last saved disk.
    let receipt = { committed: false };
    try { receipt = await runtime.finish(job, { succeeded: true, jobCompleted: true }); }
    catch (error) {
      // No room (sticky-sell 2026-10-21): the disk keeps its last saved generation; the job is not failed.
      if (ROOM[error.message]) console.log(`::notice::${ROOM[error.message]}`);
      else console.log(`::warning::Sticky disk was not saved this time: ${error.message}`);
    }
    finally {
      await runtime.release(job);
      if (await fs.readlink(mountPath) !== job.mounted.mount) throw new Error('Sticky disk path changed; refusing cleanup');
      await fs.unlink(mountPath);
      // remove refuses an active mount, including failed-detach recovery resources.
      await runtime.cleanup(job, { activeLeases: 0 });
      await fs.rm(savedConfig.root, { recursive: true });
    }
    console.log(`XENOCI_STICKY_COMMITTED generation=${receipt.generation ?? job.lease.generation} filesystem=APFS committed=${receipt.committed}`);
  } else {
    await fs.appendFile(env.GITHUB_STATE, 'XENOCI_STICKY_STARTED=true\n');
    const key = env.INPUT_KEY, input = env.INPUT_PATH;
    if (!key || !input || /[\r\n\0]/.test(input)) throw new Error('key and path are required');
    const mountPath = path.resolve(env.GITHUB_WORKSPACE, input.startsWith('~/') ? path.join(os.homedir(), input.slice(2)) : input);
    const existing = await fs.lstat(mountPath).catch((error) => { if (error.code !== 'ENOENT') throw error; return null; });
    if (existing) {
      if (!existing.isDirectory() || (await fs.readdir(mountPath)).length) throw new Error('Sticky disk path must be absent or an empty directory');
      await fs.rmdir(mountPath);
    }
    const root = await fs.mkdtemp(path.join(env.RUNNER_TEMP, 'xenoci-sticky-'));
    const runtimeConfig = { ...config, root, guestLocal: true }, runtime = createMacStickyJobRuntime({ ...runtimeConfig, capability: env.XENOCI_CACHE_JOB_CAPABILITY,
      log: (line) => console.log(`::warning::${line}`) });
    let job;
    try { job = await runtime.begin({ key, maxBytes: config.maxBytes, mode: config.mode }); }
    catch (error) {
      // No disk service (control plane or Mac image tools): the step continues with an empty plain directory.
      await fs.rm(root, { recursive: true, force: true });
      await fs.mkdir(mountPath, { recursive: true });
      // 현수 2026-10-21: sticky disks are '준비 중' on this server (503 sticky_disks_preparing); the job goes on without one.
      if (error.message === 'sticky_disks_preparing') console.log('::notice::스티키 디스크는 준비 중이라 이번 잡은 디스크 없이 돌았습니다');
      // 현수 2026-10-21 06시 (sticky-sell): no room for a new disk; nobody's disk is deleted, this job runs without one.
      else if (ROOM[error.message]) console.log(`::notice::${ROOM[error.message]}`);
      else console.log(`::warning::Sticky disk unavailable, continuing with an empty directory: ${error.message}`);
      process.exit(0);
    }
    await fs.mkdir(path.dirname(mountPath), { recursive: true });
    await fs.symlink(job.mounted.mount, mountPath);
    const state = path.join(root, 'state.json');
    await fs.writeFile(state, JSON.stringify({ job, mountPath, config: runtimeConfig }), { mode: 0o600 });
    await fs.appendFile(env.GITHUB_STATE, `XENOCI_STICKY_STATE=${state}\n`);
    if (job.readOnly) console.log(`::notice::${READ_ONLY[job.readOnly] ?? READ_ONLY.insufficient_credit}`);
    console.log(`XENOCI_STICKY_MOUNTED generation=${job.lease.generation} filesystem=APFS mode=${job.mounted.mode} restored=${job.restored} source=${job.source} bytes=${job.downloadBytes} download_ms=${job.downloadMs}`);
  }
} catch (error) {
  console.error(`::error::${error.message}${error.cause?.code ? ` (${error.cause.code})` : ''}`);
  process.exitCode = 1;
}
