import { io } from 'socket.io-client';
import { Worker } from 'node:worker_threads';
import os from 'node:os';
import { performance } from 'node:perf_hooks';
import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';

const URL_DO_PROFESSOR = 'https://cluster-distribuido-turma.onrender.com';
const SERVER_URL = process.env.SERVER_URL || URL_DO_PROFESSOR;
const cpus = os.cpus().length || 1;
const argument = process.argv.slice(2).find(arg => arg.startsWith('--workers='));
const requested = argument ? Number(argument.split('=')[1]) : Math.min(2, cpus);
if (!Number.isInteger(requested) || requested < 1 || requested > Math.min(8, cpus)) {
  throw new Error(`Use --workers=1 até --workers=${Math.min(8, cpus)}; padrão até 2.`);
}
const workers = requested;
if (!SERVER_URL) throw new Error('Configure SERVER_URL com a URL fornecida pelo professor.');
const url = new URL(SERVER_URL);
if (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(url.hostname))) {
  throw new Error('Use HTTPS na Internet; HTTP é permitido apenas em teste local.');
}

const terminal = createInterface({ input: stdin, output: stdout });
console.log('\n-------------------------------------\nCLUSTER DISTRIBUÍDO\n-------------------------------------');
const nome = process.env.ALUNO_NOME || await terminal.question('Nome: ');
const matricula = process.env.ALUNO_MATRICULA || await terminal.question('Matrícula: ');
const codigoTurma = process.env.CODIGO_TURMA || await terminal.question('Código da turma: ');
terminal.close();

console.log(`Conectando ao servidor ${SERVER_URL} | ${cpus} CPUs lógicas | ${workers} worker(s)`);
const socket = io(SERVER_URL, { auth: { role: 'worker', codigoTurma },
  transports: ['websocket'], reconnectionDelay: 2000, reconnectionDelayMax: 10000 });
let heartbeat;
let current = null;
const threads = new Set();

socket.on('connect', () => {
  console.log('🟢 CONECTADO — registrando nó...');
  socket.emit('registrar-no', { nome, matricula, plataforma: os.platform(), cpus, workers });
});

function cancel() {
  current = null;
  for (const thread of threads) thread.terminate();
  threads.clear();
}

socket.on('registro-confirmado', ({ nodeId, heartbeatMs }) => {
  console.log(`Identificação: Nó #${nodeId}\nStatus: AGUARDANDO TAREFA`);
  clearInterval(heartbeat);
  heartbeat = setInterval(() => socket.emit('heartbeat'), heartbeatMs);
});

socket.on('connect_error', error => console.error(`Conexão: ${error.message}`));
socket.on('disconnect', reason => {
  clearInterval(heartbeat); cancel();
  console.log(`🔴 DESCONECTADO (${reason}).`);
});

socket.on('tarefa-cancelada', () => { cancel(); console.log('Tarefa cancelada; aguardando próxima tarefa.'); });

socket.on('nova-tarefa', async task => {
  if (current) { console.error('Tarefa recusada: cliente ocupado'); return; }
  if (!task || task.operacao !== 'sqrt_sum' ||
      !['taskId', 'attemptId', 'experimentId'].every(key => typeof task[key] === 'string' && task[key].length <= 80) ||
      !Number.isSafeInteger(task.inicio) || !Number.isSafeInteger(task.fim) ||
      task.inicio < 1 || task.fim < task.inicio || task.fim > 1_000_000_000) {
    console.error('Tarefa inválida recusada'); return;
  }
  current = task;
  console.log(`\nNOVA TAREFA RECEBIDA\nTask: ${task.taskId}\nIntervalo: ${task.inicio.toLocaleString('pt-BR')} até ${task.fim.toLocaleString('pt-BR')}\nProcessando...`);
  const start = performance.now();
  try {
    const count = task.fim - task.inicio + 1;
    const parallelism = Math.min(workers, count);
    const base = Math.floor(count / parallelism), remainder = count % parallelism;
    let inicio = task.inicio;
    const promises = [];
    for (let index = 0; index < parallelism; index++) {
      const fim = inicio + base + (index < remainder ? 1 : 0) - 1;
      promises.push(runThread(inicio, fim)); inicio = fim + 1;
    }
    const partials = await Promise.all(promises);
    if (current !== task || !socket.connected) return;
    const processingMs = performance.now() - start;
    const resultado = partials.reduce((sum, p) => sum + p.resultado, 0);

    const kernelMs = Math.max(...partials.map(p => p.kernelMs));
    console.log(`TAREFA FINALIZADA\nTempo local: ${processingMs.toFixed(2)} ms\nResultado: ${resultado}\nEnviando resultado...`);
    socket.timeout(10000).emit('resultado-tarefa', { taskId: task.taskId, attemptId: task.attemptId,
      experimentId: task.experimentId, resultado, processingMs, kernelMs, workersUsed: parallelism }, (error, ack) => {
      console.log(!error && ack?.ok ? '✅ RESULTADO ENVIADO\nAguardando próxima tarefa.' : `Resultado sem confirmação: ${ack?.error ?? error?.message}`);
    });
    current = null;
  } catch (error) {
    if (current !== task) return;
    console.error(`Falha no processamento: ${error.message}`);
    cancel(); socket.emit('falha-tarefa', { taskId: task.taskId, attemptId: task.attemptId });
  }
});

function runThread(inicio, fim) {
  return new Promise((resolve, reject) => {
    const thread = new Worker(new URL('./worker.js', import.meta.url), { workerData: { inicio, fim } });
    threads.add(thread);
    let received = false;
    thread.once('message', result => { received = true; resolve(result); });
    thread.once('error', reject);
    thread.once('exit', code => {
      threads.delete(thread);
      if (!received) reject(new Error(`Worker encerrado sem resultado (${code})`));
    });
  });
}

for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => {
  clearInterval(heartbeat); cancel(); socket.disconnect(); process.exit(0);
});
