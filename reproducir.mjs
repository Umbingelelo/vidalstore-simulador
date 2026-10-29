// Publica el dataset de muestra en Kafka respetando los tiempos relativos de cada evento.
// --velocidad 60 reproduce la hora en un minuto; --velocidad 0 publica sin esperas.
//
//   node reproducir.mjs [--dataset dataset] [--solo actividad|compras] [--velocidad 60]
//                       [--brokers localhost:9092,localhost:9094,localhost:9096] [--cada 500]

import { parseArgs } from 'node:util';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as esperar } from 'node:timers/promises';
import { Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from '@platformatic/kafka';

const { values: opciones } = parseArgs({
  options: {
    dataset: { type: 'string', default: 'dataset' },
    solo: { type: 'string' },
    velocidad: { type: 'string', default: '60' },
    brokers: { type: 'string', default: 'localhost:9092,localhost:9094,localhost:9096' },
    cada: { type: 'string', default: '500' },
  },
});

const ARCHIVOS = {
  actividad: { archivo: 'actividad.jsonl', topic: 'vidalstore.actividad' },
  compras: { archivo: 'compras.jsonl', topic: 'vidalstore.compras' },
};

if (opciones.solo && !ARCHIVOS[opciones.solo]) salir(`--solo acepta actividad o compras, llegó «${opciones.solo}»`);
const velocidad = Number(opciones.velocidad);
if (!Number.isFinite(velocidad) || velocidad < 0) salir(`--velocidad debe ser un número mayor o igual a 0, llegó «${opciones.velocidad}»`);
const cada = Number.parseInt(opciones.cada, 10);
if (!Number.isInteger(cada) || cada < 1) salir(`--cada debe ser un entero positivo, llegó «${opciones.cada}»`);

const elegidos = opciones.solo ? [opciones.solo] : Object.keys(ARCHIVOS);
const mensajes = [];
for (const nombre of elegidos) {
  const { archivo, topic } = ARCHIVOS[nombre];
  const ruta = join(opciones.dataset, archivo);
  let texto;
  try {
    texto = readFileSync(ruta, 'utf8');
  } catch {
    salir(`No existe ${ruta}. Primero corre: node generar.mjs --salida ${opciones.dataset}`);
  }
  for (const fila of texto.split('\n')) {
    if (!fila.trim()) continue;
    const { desfaseMs, key, headers, value } = JSON.parse(fila);
    // El value se manda tal cual: si en el archivo es texto (JSON roto a propósito), viaja roto.
    mensajes.push({ desfaseMs, topic, key, headers, value: typeof value === 'string' ? value : JSON.stringify(value) });
  }
}
mensajes.sort((a, b) => a.desfaseMs - b.desfaseMs);

const producer = new Producer({
  clientId: 'vidalstore-simulador',
  bootstrapBrokers: opciones.brokers.split(',').map((b) => b.trim()),
  serializers: stringSerializers,
  idempotent: true,
  acks: ProduceAcks.ALL,
  autocreateTopics: false,
  // Idempotente y sin este límite, el cliente reintenta para siempre. Con 10 reintentos, uno por
  // segundo, aguanta la elección de un líder nuevo; si el clúster no puede confirmar, el error se ve.
  retries: 10,
  // Mismo reparto que el cliente de Java: murmur2 de la key, positivo, módulo particiones.
  partitioner: compatibilityPartitioner,
});

let cerrando = false;
process.on('SIGINT', () => {
  if (cerrando) process.exit(130);
  cerrando = true;
  console.log('\nInterrumpido. Cerrando el productor…');
});

const enviadosPorTopic = Object.fromEntries(elegidos.map((n) => [ARCHIVOS[n].topic, 0]));
const total = mensajes.length;
const duracion = velocidad === 0 ? 0 : (mensajes.at(-1)?.desfaseMs ?? 0) / velocidad;
console.log(`Publicando ${total} mensajes de ${elegidos.join(' y ')} en ${opciones.brokers}`);
console.log(velocidad === 0 ? 'Sin esperas (--velocidad 0)' : `Velocidad ${velocidad}x: unos ${Math.ceil(duracion / 1000)} s`);

// Antes de largar el reloj: conectarse y comprobar que los topics existen (el clúster no los crea solo).
const topics = elegidos.map((n) => ARCHIVOS[n].topic);
try {
  await producer.metadata({ topics });
} catch (error) {
  console.error(`No se pudo leer ${topics.join(' y ')} en ${opciones.brokers}: ${error.message}`);
  for (const causa of causas(error)) console.error(`  ${causa}`);
  console.error('¿Está arriba el clúster (docker compose up -d --wait) y corriste kafka/topics.sh?');
  await producer.close();
  process.exit(1);
}

let partida = performance.now();
let enviados = 0;
let siguienteAviso = cada;
try {
  let i = 0;
  while (i < total && !cerrando) {
    const ahora = performance.now() - partida;
    const lote = [];
    while (i < total && (velocidad === 0 || mensajes[i].desfaseMs / velocidad <= ahora) && lote.length < 1000) {
      lote.push(mensajes[i++]);
    }
    if (lote.length === 0) {
      await esperar(Math.min(250, mensajes[i].desfaseMs / velocidad - ahora));
      continue;
    }
    await producer.send({
      messages: lote.map(({ topic, key, headers, value }) => ({ topic, key, headers, value })),
    });
    for (const m of lote) enviadosPorTopic[m.topic]++;
    // El primer envío abre conexiones y pide el producerId del productor idempotente (unos 3 s).
    // El reloj parte cuando ese envío se confirma, para no comprimir el comienzo de la hora.
    if (enviados === 0 && velocidad > 0) partida = performance.now() - lote.at(-1).desfaseMs / velocidad;
    enviados += lote.length;
    if (enviados >= siguienteAviso || enviados === total) {
      const minuto = Math.floor(lote.at(-1).desfaseMs / 60_000);
      console.log(`${enviados} de ${total} · minuto ${minuto} de la hora simulada · ${Math.round(performance.now() - partida)} ms`);
      while (siguienteAviso <= enviados) siguienteAviso += cada;
    }
  }
} catch (error) {
  console.error(`Falló el envío después de ${enviados} mensajes: ${error.message}`);
  for (const causa of causas(error)) console.error(`  ${causa}`);
  process.exitCode = 1;
} finally {
  await producer.close();
}

console.log('Confirmados por el clúster (acks=all):');
for (const [topic, n] of Object.entries(enviadosPorTopic)) console.log(`  ${topic}: ${n}`);

// Los errores del cliente vienen anidados (errors y cause); se muestra cada motivo distinto una vez.
function causas(error, vistos = new Set()) {
  for (const hijo of [...(error.errors ?? []), error.cause].filter(Boolean)) {
    const texto = [hijo.apiId, hijo.message].filter(Boolean).join(': ');
    if (texto.trim()) vistos.add(texto);
    causas(hijo, vistos);
  }
  return vistos;
}

function salir(mensaje) {
  console.error(mensaje);
  process.exit(1);
}
