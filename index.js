#!/usr/bin/env node

/**
 * ═══════════════════════════════════════════════════════════════════════
 * AREBEAST — CLUSTER MASTER  (index.js)
 * ═══════════════════════════════════════════════════════════════════════
 *
 * This is the master/primary process. It does exactly ONE thing:
 * orchestrate workers. It never touches HTTP, routes, or sockets.
 *
 * WHAT IT DOES:
 *   1. Sets up the Socket.IO primary cluster adapter
 *      (lets workers broadcast events to each other's clients)
 *   2. Spawns one worker per CPU core (or 1 on free tier)
 *   3. Auto-restarts crashed workers (with back-off, up to 10 times)
 *   4. Coordinates zero-downtime graceful shutdown
 *   5. Polls worker health every 30 s
 *
 * WHAT IT DOES NOT DO:
 *   - Does not load Express, Socket.IO server, or any app logic
 *   - Does not bind to any port
 *   - Does not share memory state with workers (each worker is isolated)
 *
 * SCALING:
 *   FREE_TIER=true  → 1 worker (512 MB RAM limit)
 *   default          → 1 worker per CPU core
 *   WORKERS=n        → exactly n workers
 *
 * SOCKET.IO MULTI-WORKER ARCHITECTURE:
 *   ┌─────────────────────────────────────────────────┐
 *   │                  MASTER (index.js)              │
 *   │   setupPrimary() ← @socket.io/cluster-adapter   │
 *   │   Routes IPC messages between workers           │
 *   └──────┬──────────────┬──────────────┬────────────┘
 *          │              │              │
 *     Worker 1       Worker 2       Worker 3  ...
 *   (goat.app.js)  (goat.app.js)  (goat.app.js)
 *   setupWorker()  setupWorker()  setupWorker()
 *          │              │              │
 *    clients A,B     clients C,D    clients E,F
 *
 *   When Worker 1 calls io.emit("event"), the cluster adapter
 *   routes it through the master so Worker 2 and 3 also deliver
 *   it to their connected clients. Result: true global broadcast.
 */

import cluster   from 'node:cluster';
import os        from 'node:os';
import process   from 'node:process';
import dotenv    from 'dotenv';

dotenv.config();

// ═══════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════

const IS_FREE_TIER   = process.env.FREE_TIER === 'true';
const NUM_WORKERS    = process.env.WORKERS === 'auto'
  ? (IS_FREE_TIER ? 1 : os.cpus().length)
  : parseInt(process.env.WORKERS || '1', 10);

const MAX_MEMORY_MB        = parseInt(process.env.MAX_MEMORY_PER_WORKER || '512', 10);
const MEMORY_CHECK_INTERVAL = 30_000;   // 30 s
const RESTART_DELAY         = 2_000;    // 2 s between crash restarts
const MAX_RESTARTS          = 10;       // Before giving up on a worker
const SHUTDOWN_TIMEOUT      = 30_000;   // 30 s total graceful-shutdown budget

// ═══════════════════════════════════════════════════════════════════════
// STRUCTURED LOGGER (master process only)
// ═══════════════════════════════════════════════════════════════════════

const IS_PRODUCTION = process.env.NODE_ENV === 'production';

const log = {
  info:  (msg, meta = {}) => console.log (JSON.stringify({ level: 'INFO',  msg, ...meta, ts: Date.now() })),
  warn:  (msg, meta = {}) => console.warn(JSON.stringify({ level: 'WARN',  msg, ...meta, ts: Date.now() })),
  error: (msg, meta = {}) => console.error(JSON.stringify({ level: 'ERROR', msg, ...meta, ts: Date.now() })),
  debug: (msg, meta = {}) => IS_PRODUCTION ? undefined : console.log(JSON.stringify({ level: 'DEBUG', msg, ...meta, ts: Date.now() }))
};

// ═══════════════════════════════════════════════════════════════════════
// WORKER TRACKING
// ═══════════════════════════════════════════════════════════════════════

const workers       = new Map();   // id → worker
const startTimes    = new Map();   // id → Date.now()
const restartCounts = new Map();   // id → number

let isShuttingDown  = false;
let shutdownTimer   = null;

// ═══════════════════════════════════════════════════════════════════════
// MASTER ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════

if (cluster.isPrimary) {

  log.info('🚀 AREBEAST — Cluster Master starting', {
    pid:          process.pid,
    node:         process.version,
    platform:     process.platform,
    cpus:         os.cpus().length,
    workers:      NUM_WORKERS,
    mem_limit:    `${MAX_MEMORY_MB} MB`,
    environment:  process.env.NODE_ENV || 'development',
    free_tier:    IS_FREE_TIER
  });

  // ─── SOCKET.IO CLUSTER ADAPTER (PRIMARY SIDE) ───────────────────────
  // Must be called before any worker is spawned.
  // The adapter intercepts IPC messages from workers and re-broadcasts
  // socket events to other workers, enabling true multi-worker pub/sub.
  try {
    const { setupPrimary } = await import('@socket.io/cluster-adapter');
    setupPrimary();
    log.info('✅ Socket.IO cluster adapter primary initialised');
  } catch {
    log.info('ℹ️  Socket.IO cluster adapter not installed — single-worker / free-tier mode is fine');
  }
  // ────────────────────────────────────────────────────────────────────

  // Spawn workers
  for (let i = 1; i <= NUM_WORKERS; i++) {
    spawnWorker(i);
  }

  // Health polling
  setInterval(pollWorkerHealth, MEMORY_CHECK_INTERVAL);

  // Worker lifecycle events
  cluster.on('exit',       handleWorkerExit);
  cluster.on('online',     handleWorkerOnline);
  cluster.on('disconnect', handleWorkerDisconnect);
  cluster.on('message',    handleWorkerMessage);

  // Master process error guards — keep master alive
  process.on('uncaughtException',  (err)    => log.error('Master uncaughtException',  { error: err.message, stack: err.stack }));
  process.on('unhandledRejection', (reason) => log.error('Master unhandledRejection', { reason: String(reason) }));

  // Shutdown signals
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

  log.info(`✅ Cluster master ready — ${NUM_WORKERS} worker(s) spawned`);

} else {
  // ─── WORKER PROCESS ─────────────────────────────────────────────────
  // Import and start the worker server.
  // server.js handles everything from here.
  import('./server.js').catch(err => {
    console.error('❌ Worker failed to start:', err.message, err.stack);
    process.exit(1);
  });
}

// ═══════════════════════════════════════════════════════════════════════
// WORKER MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════

/**
 * Spawn a new worker, tagging it with a sequential WORKER_ID.
 * The first worker spawned (id=1) is the "primary worker" and is
 * responsible for disk snapshots (prevents write-race between workers).
 */
function spawnWorker(workerNum) {
  const isPrimary = (workerNum === 1);

  const worker = cluster.fork({
    WORKER_ID: String(workerNum),
    IS_PRIMARY_WORKER: isPrimary ? 'true' : 'false'
  });

  workers.set(worker.id, worker);
  startTimes.set(worker.id, Date.now());
  restartCounts.set(worker.id, 0);

  log.info('Worker spawned', {
    worker_id: worker.id,
    worker_num: workerNum,
    pid: worker.process.pid,
    is_primary: isPrimary
  });
}

function handleWorkerOnline(worker) {
  const uptime = Date.now() - (startTimes.get(worker.id) || Date.now());
  log.info('Worker online', { worker_id: worker.id, pid: worker.process.pid, startup_ms: uptime });
}

function handleWorkerDisconnect(worker) {
  log.warn('Worker disconnected', { worker_id: worker.id, pid: worker.process.pid });
}

function handleWorkerExit(worker, code, signal) {
  const uptime  = Date.now() - (startTimes.get(worker.id) || Date.now());
  const retries = restartCounts.get(worker.id) || 0;

  log.warn('Worker exited', {
    worker_id: worker.id,
    pid:       worker.process.pid,
    code,
    signal,
    uptime_s:  Math.floor(uptime / 1000),
    retries
  });

  workers.delete(worker.id);
  startTimes.delete(worker.id);

  if (isShuttingDown) {
    log.debug('Shutdown in progress — not respawning');
    return;
  }

  if (retries >= MAX_RESTARTS) {
    log.error('Worker restart limit reached — giving up', { worker_id: worker.id });
    return;
  }

  log.info('Scheduling worker respawn', { delay_ms: RESTART_DELAY, retry: retries + 1 });

  setTimeout(() => {
    if (!isShuttingDown) {
      // Re-use same worker number slot (doesn't need to be exact, just needs a num)
      const nextNum = workers.size + 1;
      restartCounts.set(worker.id, retries + 1); // track on old id (best-effort)
      spawnWorker(nextNum);
    }
  }, RESTART_DELAY);
}

function handleWorkerMessage(worker, msg) {
  if (!msg || typeof msg !== 'object') return;

  switch (msg.type) {
    case 'worker_ready':
      log.info('Worker reported ready', { worker_id: msg.worker_id, pid: msg.pid });
      break;

    case 'memory_warning':
      log.warn('Worker high-memory warning', {
        worker_id: worker.id,
        pid:       worker.process.pid,
        memory_mb: msg.memory_mb,
        threshold: msg.threshold
      });
      break;

    case 'health':
      log.debug('Worker health response', { worker_id: worker.id, ...msg.data });
      break;

    default:
      log.debug('Unknown worker message', { worker_id: worker.id, type: msg.type });
  }
}

// ═══════════════════════════════════════════════════════════════════════
// HEALTH POLLING
// ═══════════════════════════════════════════════════════════════════════

function pollWorkerHealth() {
  for (const [id, worker] of workers.entries()) {
    if (!worker.process?.pid) continue;

    try {
      worker.send({ type: 'health_check', from: 'master' });

      // Also check master's own memory (rough proxy for per-worker pressure)
      const mem = process.memoryUsage();
      const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
      if (heapMB > MAX_MEMORY_MB * 0.8) {
        log.warn('Worker memory pressure (master estimate)', {
          worker_id: id,
          heap_mb: heapMB,
          limit_mb: MAX_MEMORY_MB
        });
      }
    } catch (err) {
      log.error('Health poll failed for worker', { worker_id: id, error: err.message });
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN
// ═══════════════════════════════════════════════════════════════════════

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  log.info('🛑 Graceful shutdown initiated', { signal, workers: workers.size });

  // Hard deadline — if workers don't exit in time, force-exit
  shutdownTimer = setTimeout(() => {
    log.error('Graceful shutdown timed out — forcing exit');
    process.exit(1);
  }, SHUTDOWN_TIMEOUT);

  // Tell every worker to start its own graceful shutdown
  for (const [id, worker] of workers.entries()) {
    log.info('Signalling worker to shutdown', { worker_id: id, pid: worker.process.pid });
    try {
      worker.send({ type: 'shutdown' });
      worker.disconnect();           // Stop accepting new connections
    } catch (err) {
      log.warn('Could not signal worker', { worker_id: id, error: err.message });
    }
  }

  // Wait for all workers to fully exit
  await new Promise((resolve) => {
    const check = setInterval(() => {
      const alive = [...workers.values()].filter(w => !w.isDead());
      if (alive.length === 0) {
        clearInterval(check);
        resolve();
      } else {
        log.debug('Waiting for workers', { alive: alive.length });
      }
    }, 500);
  });

  clearTimeout(shutdownTimer);
  log.info('✅ All workers exited — master shutting down');
  process.exit(0);
}

// ═══════════════════════════════════════════════════════════════════════
// EXPORTS (for testing)
// ═══════════════════════════════════════════════════════════════════════

export { NUM_WORKERS, MAX_MEMORY_MB };
