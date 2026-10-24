);
    // The owner's trial terms: a missing or unreachable stored image never fails the build; the job starts an empty
    // disk and its commit writes the next generation (restored stays false).
    let restored = false, source = 'empty', downloadMs = 0, downloadBytes = 0;
    try {
      if (lease.generation > 0 && lease.read_url) {
        const started = Date.now();
        try {
          const readUrl = signedUrl(lease.read_url);
          const response = await fetchImpl(readUrl, { redirect: 'error', headers: viaHeaders(readUrl) });
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
      return { lease, mounted, maxBytes, localId, restored, source, downloadMs, downloadBytes };
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
