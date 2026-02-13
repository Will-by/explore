/**
 * ═══════════════════════════════════════════════════════════════════════
 * AREBEAST — CLUSTER WORKER  (server.js)
 * ═══════════════════════════════════════════════════════════════════════
 *
 * One copy of this file runs per CPU core (spawned by index.js).
 * Each worker:
 *   1. Imports and initialises the GOAT app (goat.app.js)
 *   2. Runs the cold-start sequence (disk → Neon if needed)
 *   3. Binds the HTTP server to the shared port via Node cluster
 *   4. Handles its own graceful shutdown (disk snapshot + DB sync)
 *   5. Reports health metrics to the master process
 *
 * WHAT THIS FILE DOES NOT DO:
 *   - Does not create any routes (all in goat.app.js)
 *   - Does not manage other workers (index.js does that)
 *   - Does not do OS-level load balancing (cluster/kernel handles it)
 *
 * PORT SHARING:
 *   Node.js cluster distributes incoming connections across workers using
 *   the OS scheduler. No extra configuration needed — just listen on the
 *   same port from every worker.
 */

import process from 'node:process';
import { server, initializeApp, syncDirtyRecordsToNeon, eternalStore, pool, config } from './goat.app.js';

// ═══════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════

const PORT = parseInt(process.env.PORT || config.port || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const WORKER_ID = process.env.WORKER_ID || '1';

// ═══════════════════════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN STATE
// ═══════════════════════════════════════════════════════════════════════

let isShuttingDown = false;

// Track open sockets so we can destroy them on forced shutdown
const activeConnections = new Set();
server.on('connection', (socket) => {
  activeConnections.add(socket);
  socket.on('close', () => activeConnections.delete(socket));
});

// ═══════════════════════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN
// ═══════════════════════════════════════════════════════════════════════

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log(`🛑 [Worker ${WORKER_ID}] Shutdown (${signal}) — saving state...`);

  // 1. Stop accepting new HTTP connections
  server.close();

  try {
    // 2. Flush memory store to disk (primary worker only — avoids write races)
    if (process.env.IS_PRIMARY_WORKER === 'true') {
      console.log(`💾 [Worker ${WORKER_ID}] Saving disk snapshot...`);
      await eternalStore.snapshotToDisk();
    }

    // 3. Sync dirty records to Neon (each worker tracks its own dirty set)
    console.log(`🔄 [Worker ${WORKER_ID}] Syncing dirty records to Neon...`);
    await syncDirtyRecordsToNeon();

    console.log(`✅ [Worker ${WORKER_ID}] State saved cleanly`);
  } catch (err) {
    console.error(`❌ [Worker ${WORKER_ID}] Shutdown save error:`, err.message);
  }

  // 4. Wait up to 20 s for in-flight requests to finish, then force-close
  const forceClose = setTimeout(() => {
    console.warn(`⚡ [Worker ${WORKER_ID}] Forcing close — ${activeConnections.size} connections remaining`);
    for (const socket of activeConnections) socket.destroy();
  }, 20_000);

  const waitForDrain = setInterval(() => {
    if (activeConnections.size === 0) {
      clearInterval(waitForDrain);
      clearTimeout(forceClose);

      // 5. Close the DB pool and exit cleanly
      pool.end(() => {
        console.log(`✅ [Worker ${WORKER_ID}] DB pool closed — exiting`);
        process.exit(0);
      });
    }
  }, 500);
}

// ═══════════════════════════════════════════════════════════════════════
// MESSAGE HANDLER  (from cluster master)
// ═══════════════════════════════════════════════════════════════════════

process.on('message', (msg) => {
  if (!msg || typeof msg !== 'object') return;

  switch (msg.type) {

    // Master requested a clean shutdown
    case 'shutdown':
      gracefulShutdown('MASTER_REQUEST');
      break;

    // Master is polling health metrics
    case 'health_check': {
      const mem = process.memoryUsage();
      const store = eternalStore.getStats();
      process.send?.({
        type: 'health',
        data: {
          worker_id: WORKER_ID,
          pid: process.pid,
          uptime_s: Math.floor(process.uptime()),
          connections: activeConnections.size,
          memory: {
            heap_used_mb: Math.round(mem.heapUsed / 1024 / 1024),
            heap_total_mb: Math.round(mem.heapTotal / 1024 / 1024),
            rss_mb: Math.round(mem.rss / 1024 / 1024)
          },
          store: {
            users: store.users,
            posts: store.posts,
            dirty_users: store.dirtyUsers,
            dirty_posts: store.dirtyPosts,
            cache_hit_rate: store.cacheHitRate,
            db_syncs: store.dbSyncs
          }
        }
      });
      break;
    }
  }
});

// ═══════════════════════════════════════════════════════════════════════
// SIGNAL HANDLERS
// ═══════════════════════════════════════════════════════════════════════

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

// Keep worker alive on unhandled errors — let the master decide to kill
process.on('uncaughtException', (err) => {
  console.error(`❌ [Worker ${WORKER_ID}] Uncaught exception:`, err.message, err.stack);
});

process.on('unhandledRejection', (reason) => {
  console.error(`❌ [Worker ${WORKER_ID}] Unhandled rejection:`, String(reason));
});

// ═══════════════════════════════════════════════════════════════════════
// BOOT SEQUENCE
// ═══════════════════════════════════════════════════════════════════════

async function startWorker() {
  try {
    console.log(`🔧 [Worker ${WORKER_ID} / PID ${process.pid}] Booting...`);

    // 1. Run the GOAT cold-start (disk load → Neon cold load → Turso init)
    await initializeApp();

    // 2. Bind the HTTP server to the port
    //    Cluster lets all workers share the same port — OS distributes connections.
    await new Promise((resolve, reject) => {
      server.listen(PORT, HOST, (err) => {
        if (err) return reject(err);
        resolve();
      });
    });

    console.log(`
💀🔥 ETERNAL GOD TIER WORKER ONLINE 💀🔥
  Worker  : ${WORKER_ID} (PID ${process.pid})
  Port    : ${PORT}
  Host    : ${HOST}
  Env     : ${process.env.NODE_ENV || 'development'}
  Memory  : ${eternalStore.getStats().users} users / ${eternalStore.getStats().posts} posts in RAM
  Primary : ${process.env.IS_PRIMARY_WORKER === 'true' ? 'YES (handles disk snapshots)' : 'NO'}

⚡ READY — accepting connections
    `);

    // Notify master that we're online (triggers handleWorkerOnline in index.js)
    process.send?.({ type: 'worker_ready', worker_id: WORKER_ID, pid: process.pid });

  } catch (err) {
    console.error(`❌ [Worker ${WORKER_ID}] Fatal boot error:`, err.message, err.stack);
    process.exit(1);
  }
}

startWorker();
