import { parentPort, workerData } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

// Operação fixa instalada localmente. O servidor fornece somente intervalos.
const { inicio, fim } = workerData;
if (!Number.isSafeInteger(inicio) || !Number.isSafeInteger(fim) ||
    inicio < 1 || fim < inicio || fim > 1_000_000_000) {
  throw new Error('Intervalo inválido');
}
const start = performance.now();
let resultado = 0;
for (let i = inicio; i <= fim; i++) resultado += Math.sqrt(i);
parentPort.postMessage({ resultado, kernelMs: performance.now() - start });