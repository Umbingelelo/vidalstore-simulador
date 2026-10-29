// Publica el dataset de la venta de verano en Kafka respetando los tiempos de cada evento.
//
//   node reproducir.mjs [opciones]          node reproducir.mjs --ayuda
//
// Cada opción se puede dar de tres formas, y gana la primera que aparezca:
//   1. en la línea de comandos:        --velocidad 600
//   2. como variable de entorno:       SIM_VELOCIDAD=600 node reproducir.mjs
//   3. en un archivo .env en la carpeta del simulador (ver .env.example)
//
// Si algo falla, el simulador explica qué pasó, por qué suele pasar y qué hacer, y termina con un
// código de salida distinto según el tipo de problema (ver CODIGOS al final de las opciones).

import { parseArgs } from 'node:util';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as esperar } from 'node:timers/promises';
import { Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from '@platformatic/kafka';

// ---------------------------------------------------------------------------------------------
// 1. Las opciones
// ---------------------------------------------------------------------------------------------

const OPCIONES = [
  { nombre: 'brokers', env: 'SIM_BROKERS', tipo: 'brokers', defecto: 'localhost:9092,localhost:9094,localhost:9096',
    ayuda: 'Bootstrap del clúster, host:puerto separados por coma' },
  { nombre: 'velocidad', env: 'SIM_VELOCIDAD', tipo: 'numero', min: 0, defecto: '60',
    ayuda: 'Factor sobre el tiempo real: 60 = la hora en un minuto; 0 = todo sin esperas' },
  { nombre: 'dataset', env: 'SIM_DATASET', tipo: 'texto', defecto: 'dataset',
    ayuda: 'Carpeta con actividad.jsonl, compras.jsonl y resumen.json' },
  { nombre: 'solo', env: 'SIM_SOLO', tipo: 'eleccion', valores: ['actividad', 'compras'], defecto: '',
    ayuda: 'Publicar un solo archivo: actividad o compras (vacío = los dos)' },
  { nombre: 'desde', env: 'SIM_DESDE', tipo: 'entero', min: 0, max: 59, defecto: '0',
    ayuda: 'Minuto de la noche desde el que se publica (0 = 21:00)' },
  { nombre: 'hasta', env: 'SIM_HASTA', tipo: 'entero', min: 1, max: 60, defecto: '60',
    ayuda: 'Minuto de la noche hasta el que se publica, sin incluirlo (60 = 22:00)' },
  { nombre: 'repetir', env: 'SIM_REPETIR', tipo: 'entero', min: 1, max: 100, defecto: '1',
    ayuda: 'Cuántas veces se publica el tramo elegido, una tras otra' },
  { nombre: 'sin-invalidos', env: 'SIM_SIN_INVALIDOS', tipo: 'booleano', defecto: 'no',
    ayuda: 'Omitir los 34 eventos inválidos puestos a propósito' },
  { nombre: 'sin-duplicados', env: 'SIM_SIN_DUPLICADOS', tipo: 'booleano', defecto: 'no',
    ayuda: 'Omitir los 17 duplicados puestos a propósito' },
  { nombre: 'topic-actividad', env: 'SIM_TOPIC_ACTIVIDAD', tipo: 'texto', defecto: 'vidalstore.actividad',
    ayuda: 'Topic donde va actividad.jsonl (para probar en un topic aparte)' },
  { nombre: 'topic-compras', env: 'SIM_TOPIC_COMPRAS', tipo: 'texto', defecto: 'vidalstore.compras',
    ayuda: 'Topic donde va compras.jsonl' },
  { nombre: 'cada', env: 'SIM_CADA', tipo: 'entero', min: 1, max: 1_000_000, defecto: '500',
    ayuda: 'Cada cuántos mensajes se muestra el avance' },
  { nombre: 'timeout-conexion', env: 'SIM_TIMEOUT_CONEXION', tipo: 'entero', min: 1000, max: 120_000, defecto: '5000',
    ayuda: 'Milisegundos que se espera a que un broker acepte la conexión' },
  { nombre: 'reintentos', env: 'SIM_REINTENTOS', tipo: 'entero', min: 0, max: 50, defecto: '10',
    ayuda: 'Reintentos del cliente de Kafka por cada envío (cubren la elección de un líder)' },
  { nombre: 'espera-reintento', env: 'SIM_ESPERA_REINTENTO', tipo: 'entero', min: 100, max: 60_000, defecto: '1000',
    ayuda: 'Milisegundos entre esos reintentos' },
  { nombre: 'reintentos-lote', env: 'SIM_REINTENTOS_LOTE', tipo: 'entero', min: 0, max: 20, defecto: '3',
    ayuda: 'Si un lote falla igual, cuántas veces se espera al clúster y se vuelve a mandar' },
  { nombre: 'espera-lote', env: 'SIM_ESPERA_LOTE', tipo: 'entero', min: 1, max: 600, defecto: '10',
    ayuda: 'Segundos que se espera al clúster antes de volver a mandar un lote' },
  { nombre: 'client-id', env: 'SIM_CLIENT_ID', tipo: 'texto', defecto: 'vidalstore-simulador',
    ayuda: 'client.id del productor, como aparece en los logs de los brokers' },
  { nombre: 'seco', env: 'SIM_SECO', tipo: 'booleano', defecto: 'no',
    ayuda: 'Ensayo en seco: valida todo y muestra qué se publicaría, sin conectarse a Kafka' },
];

const CODIGOS = {
  ok: 0,             // todo confirmado por el clúster
  configuracion: 1,  // una opción con un valor que no sirve, o el dataset falta o está dañado
  conexion: 2,       // no se pudo conectar, o faltan los topics: no se publicó nada
  aMitad: 3,         // se publicó una parte y el clúster dejó de confirmar
  interrumpido: 130, // Ctrl+C
};

// El .env es opcional. process.loadEnvFile no pisa las variables que ya vienen del entorno.
if (existsSync('.env')) {
  try {
    process.loadEnvFile('.env');
  } catch (error) {
    problema('configuracion', {
      titulo: 'El archivo .env no se pudo leer',
      que: `.env existe en ${process.cwd()} pero no tiene el formato NOMBRE=valor (${error.message}).`,
      porque: 'Suele pasar con comillas sin cerrar o con un editor que lo guardó con otra codificación.',
      hacer: ['Compararlo con .env.example y dejar una variable por línea, sin espacios alrededor del =.',
        'Para descartarlo, renombrarlo (mv .env .env.roto) y volver a correr.'],
    });
  }
}

let argumentos;
try {
  argumentos = parseArgs({
    options: {
      ayuda: { type: 'boolean', short: 'h' },
      ...Object.fromEntries(OPCIONES.map((o) => [o.nombre, { type: o.tipo === 'booleano' ? 'boolean' : 'string' }])),
    },
  }).values;
} catch (error) {
  problema('configuracion', {
    titulo: 'Una opción de la línea de comandos no existe o le falta su valor',
    que: error.message,
    porque: 'Las opciones van con dos guiones y su valor después: --velocidad 600.',
    hacer: ['Ver las opciones válidas con: node reproducir.mjs --ayuda'],
  });
}
if (argumentos.ayuda) {
  mostrarAyuda();
  process.exit(CODIGOS.ok);
}

const config = {};
const origen = {};
for (const o of OPCIONES) {
  let crudo = o.defecto;
  origen[o.nombre] = 'por defecto';
  if (process.env[o.env] !== undefined && process.env[o.env] !== '') {
    crudo = process.env[o.env];
    origen[o.nombre] = `variable ${o.env}`;
  }
  if (argumentos[o.nombre] !== undefined) {
    crudo = argumentos[o.nombre];
    origen[o.nombre] = `--${o.nombre}`;
  }
  config[o.nombre] = validar(o, crudo);
}
if (config.desde >= config.hasta) {
  problema('configuracion', {
    titulo: 'El tramo de la noche está vacío',
    que: `desde = ${config.desde} y hasta = ${config.hasta}: no queda ningún minuto entre los dos.`,
    porque: '--desde es el primer minuto que se publica y --hasta el primero que ya no.',
    hacer: ['Para la ráfaga de errores de las 21:30 a 21:35: --desde 30 --hasta 35.'],
  });
}

// ---------------------------------------------------------------------------------------------
// 2. El dataset
// ---------------------------------------------------------------------------------------------

const ARCHIVOS = {
  actividad: { archivo: 'actividad.jsonl', topic: config['topic-actividad'] },
  compras: { archivo: 'compras.jsonl', topic: config['topic-compras'] },
};
const elegidos = config.solo ? [config.solo] : Object.keys(ARCHIVOS);

// resumen.json dice qué líneas son inválidas o duplicadas; solo hace falta si se pide omitirlas.
const omitir = new Set();
if (config['sin-invalidos'] || config['sin-duplicados']) {
  const ruta = join(config.dataset, 'resumen.json');
  let resumen;
  try {
    resumen = JSON.parse(readFileSync(ruta, 'utf8'));
  } catch (error) {
    problema('configuracion', {
      titulo: 'No se pudo leer resumen.json',
      que: `${ruta}: ${error.code === 'ENOENT' ? 'no existe' : error.message}.`,
      porque: '--sin-invalidos y --sin-duplicados usan resumen.json para saber qué líneas omitir.',
      hacer: ['Recuperar el dataset original: git checkout -- dataset/',
        'O regenerarlo junto con resumen.json: node generar.mjs'],
    });
  }
  const { invalidos = [], duplicados = [] } = resumen.anomalias ?? {};
  if (config['sin-invalidos']) for (const a of invalidos) for (const l of a.lineas) omitir.add(`${a.archivo}:${l}`);
  // De cada duplicado se conserva la primera línea y se omiten las repeticiones.
  if (config['sin-duplicados']) for (const a of duplicados) for (const l of a.lineas.slice(1)) omitir.add(`${a.archivo}:${l}`);
}

const inicioMs = config.desde * 60_000;
const finMs = config.hasta * 60_000;
const mensajes = [];
let omitidos = 0;
for (const nombre of elegidos) {
  const { archivo, topic } = ARCHIVOS[nombre];
  const ruta = join(config.dataset, archivo);
  let texto;
  try {
    texto = readFileSync(ruta, 'utf8');
  } catch (error) {
    problema('configuracion', {
      titulo: `No se encontró ${archivo}`,
      que: `${ruta}: ${error.code === 'ENOENT' ? 'no existe' : error.message}.`,
      porque: `El simulador busca el dataset en la carpeta «${config.dataset}», relativa a donde se ejecuta.`,
      hacer: ['Correrlo desde la carpeta del simulador: cd vidalstore-simulador',
        'Si se borró el dataset: git checkout -- dataset/   o   node generar.mjs',
        'Si está en otra carpeta: --dataset <carpeta> o SIM_DATASET=<carpeta>'],
    });
  }
  const filas = texto.split('\n');
  for (let n = 0; n < filas.length; n++) {
    const fila = filas[n];
    if (!fila.trim()) continue;
    let linea;
    try {
      linea = JSON.parse(fila);
      if (typeof linea.desfaseMs !== 'number' || linea.value === undefined) throw new Error('faltan desfaseMs o value');
    } catch (error) {
      problema('configuracion', {
        titulo: `La línea ${n + 1} de ${archivo} está dañada`,
        que: `No es una línea del simulador: ${error.message}.`,
        porque: 'Los eventos inválidos a propósito vienen dentro de "value"; la línea completa siempre es JSON. ' +
          'Si no lo es, el archivo se editó a mano o la descarga quedó cortada.',
        hacer: ['Recuperar el original: git checkout -- dataset/   (o volver a descargar el repositorio)',
          'O regenerarlo: node generar.mjs'],
      });
    }
    if (linea.desfaseMs < inicioMs || linea.desfaseMs >= finMs) continue;
    if (omitir.has(`${archivo}:${n + 1}`)) { omitidos++; continue; }
    const { desfaseMs, key, headers, value } = linea;
    // El value se manda tal cual: si en el archivo es texto (JSON roto a propósito), viaja roto.
    mensajes.push({ desfaseMs: desfaseMs - inicioMs, minuto: Math.floor(desfaseMs / 60_000), topic, key, headers,
      value: typeof value === 'string' ? value : JSON.stringify(value) });
  }
}
mensajes.sort((a, b) => a.desfaseMs - b.desfaseMs);
const topics = elegidos.map((n) => ARCHIVOS[n].topic);

if (mensajes.length === 0) {
  problema('configuracion', {
    titulo: 'No hay nada que publicar',
    que: `Entre el minuto ${config.desde} y el ${config.hasta} no quedó ningún mensaje de ${elegidos.join(' y ')}.`,
    porque: 'El tramo es muy corto o todo lo que había en él se omitió.',
    hacer: ['Ampliar el tramo con --desde y --hasta, o quitar --sin-invalidos y --sin-duplicados.'],
  });
}

// ---------------------------------------------------------------------------------------------
// 3. Lo que se va a hacer
// ---------------------------------------------------------------------------------------------

const total = mensajes.length;
const duracionMs = config.velocidad === 0 ? 0 : mensajes.at(-1).desfaseMs / config.velocidad;
console.log('Simulador de la venta de verano de VidalStore');
for (const o of OPCIONES) {
  const v = config[o.nombre];
  const valor = typeof v === 'boolean' ? (v ? 'sí' : 'no') : Array.isArray(v) ? v.join(',') : o.nombre === 'solo' && !v ? '(los dos)' : v;
  if (origen[o.nombre] !== 'por defecto' || ['brokers', 'velocidad', 'desde', 'hasta'].includes(o.nombre)) {
    console.log(`  ${o.nombre.padEnd(17)} ${String(valor).padEnd(46)} ${origen[o.nombre]}`);
  }
}
console.log(`Se publican ${total} mensajes por vuelta (${porTopic(mensajes)})` +
  (omitidos ? `, omitiendo ${omitidos}` : '') + (config.repetir > 1 ? `, ${config.repetir} vueltas` : '') + '.');
console.log(config.velocidad === 0 ? 'Sin esperas (velocidad 0).'
  : `Velocidad ${config.velocidad}x: unos ${Math.ceil(duracionMs / 1000)} s por vuelta.`);

if (config.seco) {
  console.log('\nEnsayo en seco: no se conectó a Kafka ni se publicó nada. El dataset y las opciones están bien.');
  process.exit(CODIGOS.ok);
}

// ---------------------------------------------------------------------------------------------
// 4. Conectarse y comprobar los topics
// ---------------------------------------------------------------------------------------------

const producer = new Producer({
  clientId: config['client-id'],
  bootstrapBrokers: config.brokers,
  serializers: stringSerializers,
  idempotent: true,
  acks: ProduceAcks.ALL,
  autocreateTopics: false,
  connectTimeout: config['timeout-conexion'],
  // Idempotente y sin este límite, el cliente reintentaría para siempre: con un tope, si el clúster
  // no puede confirmar, el error se ve y se explica.
  retries: config.reintentos,
  retryDelay: config['espera-reintento'],
  // Mismo reparto que el cliente de Java: murmur2 de la key, positivo, módulo particiones.
  partitioner: compatibilityPartitioner,
});

let interrumpido = false;
process.on('SIGINT', () => {
  if (interrumpido) process.exit(CODIGOS.interrumpido);
  interrumpido = true;
  console.log('\nCtrl+C: se termina el lote en curso y se cierra el productor. Ctrl+C otra vez para salir ya.');
});
process.on('unhandledRejection', (error) => {
  problema('aMitad', explicar(error, 'durante'));
});

const confirmados = Object.fromEntries(topics.map((t) => [t, 0]));
let ultimoMinuto = config.desde;

try {
  await producer.metadata({ topics });
} catch (error) {
  await cerrar();
  problema('conexion', explicar(error, 'antes'));
}

// ---------------------------------------------------------------------------------------------
// 5. Publicar
// ---------------------------------------------------------------------------------------------

let enviados = 0;
const t0 = performance.now();
for (let vuelta = 1; vuelta <= config.repetir && !interrumpido; vuelta++) {
  if (config.repetir > 1) console.log(`Vuelta ${vuelta} de ${config.repetir}`);
  let partida = performance.now();
  let primero = true;
  let siguienteAviso = config.cada;
  let enVuelta = 0;
  let i = 0;
  while (i < total && !interrumpido) {
    const ahora = performance.now() - partida;
    const lote = [];
    while (i < total && (config.velocidad === 0 || mensajes[i].desfaseMs / config.velocidad <= ahora) && lote.length < 1000) {
      lote.push(mensajes[i++]);
    }
    if (lote.length === 0) {
      await esperar(Math.min(250, mensajes[i].desfaseMs / config.velocidad - ahora));
      continue;
    }

    const pausa = await enviarConEspera(lote);
    // El tiempo esperando al clúster no cuenta: la noche sigue desde donde quedó, sin ráfagas.
    partida += pausa;
    for (const m of lote) confirmados[m.topic]++;
    ultimoMinuto = lote.at(-1).minuto;
    // El primer envío abre conexiones y pide el producerId del productor idempotente (unos 3 s).
    // El reloj parte cuando ese envío se confirma, para no comprimir el comienzo de la hora.
    if (primero && config.velocidad > 0) partida = performance.now() - lote.at(-1).desfaseMs / config.velocidad;
    primero = false;
    enVuelta += lote.length;
    enviados += lote.length;
    if (enVuelta >= siguienteAviso || enVuelta === total) {
      console.log(`${enVuelta} de ${total} · ${hora(ultimoMinuto)} de la noche · ${segundos(performance.now() - partida)}`);
      while (siguienteAviso <= enVuelta) siguienteAviso += config.cada;
    }
  }
}

await cerrar();
resumenFinal();
if (interrumpido) {
  console.log(`\nInterrumpido en ${hora(ultimoMinuto)}. Para seguir desde ahí:`);
  console.log(`  node reproducir.mjs --desde ${ultimoMinuto}${config.hasta < 60 ? ` --hasta ${config.hasta}` : ''}`);
  console.log('El minuto en curso se vuelve a publicar en parte: los consumidores lo descartan por eventoId.');
  process.exit(CODIGOS.interrumpido);
}
console.log(`Listo en ${segundos(performance.now() - t0)}: el clúster confirmó todo con acks=all.`);
process.exit(CODIGOS.ok);

// ---------------------------------------------------------------------------------------------
// Funciones
// ---------------------------------------------------------------------------------------------

// Manda un lote. Si falla por algo de lo que el clúster se puede recuperar (un broker que se cae y
// vuelve, una elección de líder, una red que se corta un momento), avisa, espera y lo vuelve a
// mandar. Devuelve los milisegundos que pasó esperando.
async function enviarConEspera(lote) {
  let pausa = 0;
  for (let intento = 0; ; intento++) {
    try {
      await producer.send({ messages: lote.map(({ topic, key, headers, value }) => ({ topic, key, headers, value })) });
      if (intento > 0) console.log(`El clúster volvió a confirmar. Se sigue desde ${hora(lote[0].minuto)}.`);
      return pausa;
    } catch (error) {
      const e = explicar(error, 'durante');
      if (!e.recuperable || intento >= config['reintentos-lote'] || interrumpido) {
        await cerrar();
        resumenFinal();
        e.hacer = [...e.hacer,
          `Cuando esté resuelto, seguir desde donde quedó: node reproducir.mjs --desde ${lote[0].minuto}`];
        problema('aMitad', e);
      }
      console.error(`\nAVISO · ${e.titulo}`);
      console.error(`  ${e.que}`);
      console.error(`  Se espera ${config['espera-lote']} s a que el clúster se recupere y se vuelve a mandar el lote` +
        ` (intento ${intento + 1} de ${config['reintentos-lote']}). Ctrl+C para detener.`);
      const t = performance.now();
      await esperar(config['espera-lote'] * 1000);
      pausa += performance.now() - t;
    }
  }
}

// Traduce un error del cliente de Kafka o de la red a qué pasó, por qué y qué hacer.
// momento es 'antes' (al conectarse y leer los topics) o 'durante' (publicando).
function explicar(error, momento) {
  const detalle = [...detalles(error)];
  const texto = [error?.message, ...detalle].join(' | ');
  const tiene = (...claves) => claves.some((c) => texto.includes(c));
  const brokers = config.brokers.join(',');
  const enLocal = config.brokers.every((b) => /^(localhost|127\.0\.0\.1)/.test(b));
  const base = { detalle, recuperable: false };

  if (tiene('UNKNOWN_TOPIC_OR_PARTITION', 'Unknown topic')) {
    return { ...base, titulo: 'Falta un topic en el clúster',
      que: `El clúster responde, pero no tiene ${topics.join(' o ')}.`,
      porque: 'El clúster no crea topics al vuelo (auto.create.topics.enable=false): los crea topics.sh. ' +
        'También pasa si se hizo docker compose down -v, que borra los topics con los datos.',
      hacer: enLocal
        ? ['En vidalstore-plataforma: sh kafka/topics.sh', 'Comprobar: los topics aparecen en Kafka UI']
        : ['En mq-1: sudo SERVICIO=broker BOOTSTRAP=10.0.21.11:9092 sh topics.sh'] };
  }
  if (tiene('NOT_ENOUGH_REPLICAS')) {
    return { ...base, recuperable: true, titulo: 'El clúster no tiene réplicas suficientes para confirmar',
      que: 'Los brokers rechazan la escritura: quedan menos de 2 réplicas al día (NOT_ENOUGH_REPLICAS).',
      porque: 'Los topics tienen min.insync.replicas=2 y el productor usa acks=all: con dos brokers caídos, ' +
        'Kafka prefiere rechazar antes que guardar un evento en una sola copia. Es el comportamiento correcto.',
      hacer: enLocal
        ? ['Ver qué broker falta: docker compose ps (en vidalstore-plataforma)', 'Levantarlo: docker compose up -d --wait']
        : ['Ver qué mq tiene Kafka abajo (consola de EC2 o sudo docker ps en cada mq)',
            'Levantarlo en esa mq: sudo docker compose --env-file mq-N.env up -d --wait'] };
  }
  if (tiene('LEADER_NOT_AVAILABLE', 'NOT_LEADER_OR_FOLLOWER', 'FENCED_LEADER_EPOCH')) {
    return { ...base, recuperable: true, titulo: 'Una partición está sin líder',
      que: 'El broker que era líder de una partición se cayó y todavía no hay otro elegido.',
      porque: 'Los controllers eligen un líder nuevo entre las réplicas del ISR en pocos segundos. Si no ' +
        'termina, es que no queda ninguna réplica al día o los controllers no tienen mayoría (2 de 3).',
      hacer: [enLocal ? 'docker compose ps: los 3 controllers y al menos 2 brokers en (healthy)'
        : 'En las mq: Kafka arriba en al menos dos, y los tres controllers'] };
  }
  if (tiene('ENOTFOUND', 'EAI_AGAIN', 'getaddrinfo')) {
    return { ...base, titulo: 'No se encuentra el nombre de un broker',
      que: `Ningún nombre de ${brokers} se pudo resolver a una IP.`,
      porque: 'Nombres como broker-1 solo existen dentro de la red de Docker. Desde el equipo se usa ' +
        'localhost con los puertos publicados; en la nube, las IP privadas de las mq.',
      hacer: ['En el equipo: --brokers localhost:9092,localhost:9094,localhost:9096',
        'En gestion: --brokers 10.0.21.11:9092,10.0.22.12:9092,10.0.21.13:9092'] };
  }
  if (tiene('ECONNREFUSED')) {
    return { ...base, recuperable: momento === 'durante', titulo: 'Ningún broker acepta la conexión',
      que: `En ${brokers} no hay nada escuchando (ECONNREFUSED).`,
      porque: momento === 'durante'
        ? 'Los brokers se cayeron mientras se publicaba: se detuvo el compose, Docker se cerró o la máquina se reinició.'
        : 'El clúster no está arriba, todavía está arrancando, o publica otros puertos.',
      hacer: enLocal
        ? ['En vidalstore-plataforma: docker compose up -d --wait', 'docker compose ps: los brokers en (healthy)',
            'Si el grupo cambió los puertos: --brokers con los suyos']
        : ['En cada mq: sudo docker ps; si Kafka no está, levantarlo con su compose',
            'Las IP de --brokers tienen que ser las de docs/red.md'] };
  }
  if (tiene('ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'Timeout', 'timed out', 'PLT_KFK_TIMEOUT')) {
    return { ...base, recuperable: momento === 'durante', titulo: 'Los brokers no responden a tiempo',
      que: `Se intentó conectar con ${brokers} y no hubo respuesta en ${config['timeout-conexion']} ms.`,
      porque: enLocal
        ? 'Docker Desktop está detenido, en pausa o sin memoria y los brokers se quedaron colgados.'
        : 'En la nube casi siempre es la red: el Security Group de las mq no acepta al de gestion en el ' +
          '9092, o las IP no son las de las mq.',
      hacer: enLocal
        ? ['Abrir Docker Desktop y revisar que esté corriendo; docker compose ps',
            'Si falta memoria: docker compose stop kafka-ui y volver a intentar']
        : ['Desde gestion: timeout 3 bash -c "</dev/tcp/10.0.21.11/9092" && echo abierto',
            'Revisar el Security Group de las mq: entrada 9092 desde el Security Group de gestion',
            `Si la red es lenta: --timeout-conexion 15000`] };
  }
  if (tiene('PLT_KFK_NETWORK', 'ECONNRESET', 'EPIPE', 'Connection closed', 'socket')) {
    return { ...base, recuperable: true, titulo: 'Se cortó la conexión con un broker',
      que: 'Un broker cerró la conexión mientras se publicaba.',
      porque: 'Un broker se reinició o se detuvo, o la red entre las máquinas se cortó.',
      hacer: [enLocal ? 'docker compose ps y docker compose logs broker-1 --tail 30'
        : 'Revisar en la consola de EC2 que las mq sigan encendidas'] };
  }
  return { ...base, titulo: 'Error que el simulador no reconoce',
    que: error?.message ?? String(error),
    porque: 'No es uno de los casos conocidos. El detalle técnico de abajo dice qué respondió Kafka.',
    hacer: ['Buscar el código en mayúsculas del detalle (por ejemplo, NOT_LEADER_OR_FOLLOWER) en la documentación de Kafka',
      'Llevar esta salida completa a la consulta del jueves'] };
}

// Los errores del cliente vienen anidados (errors y cause); se junta cada motivo distinto una vez.
function detalles(error, vistos = new Set()) {
  if (!error || typeof error !== 'object') return vistos;
  for (const hijo of [...(Array.isArray(error.errors) ? error.errors : []), error.cause].filter(Boolean)) {
    const texto = [hijo.apiId, hijo.code, hijo.message].filter(Boolean).join(': ');
    if (texto.trim()) vistos.add(texto);
    detalles(hijo, vistos);
  }
  return vistos;
}

// Muestra un problema con el mismo formato siempre y termina con su código de salida.
function problema(tipo, { titulo, que, porque, hacer = [], detalle = [] }) {
  const linea = '-'.repeat(78);
  console.error(`\n${linea}\nERROR · ${titulo}\n${linea}`);
  console.error(`Qué pasó:   ${que}`);
  console.error(`Por qué:    ${porque}`);
  hacer.forEach((h, n) => console.error(`${n === 0 ? 'Qué hacer:' : '          '}  ${n + 1}. ${h}`));
  if (detalle.length) {
    console.error('Detalle técnico:');
    for (const d of detalle) console.error(`  ${d}`);
  }
  console.error(`${linea}\nCódigo de salida ${CODIGOS[tipo]} (${tipo}).`);
  process.exit(CODIGOS[tipo]);
}

function validar(o, crudo) {
  const malo = (aceptado) => problema('configuracion', {
    titulo: `Valor no válido para ${o.nombre}`,
    que: `${o.nombre} vale «${crudo}» (${origen[o.nombre]}).`,
    porque: `${o.ayuda}. Acepta ${aceptado}.`,
    hacer: [`Corregirlo en ${origen[o.nombre] === 'por defecto' ? 'el código' : origen[o.nombre]}`,
      'Ver todas las opciones: node reproducir.mjs --ayuda'],
  });
  const t = String(crudo).trim();
  switch (o.tipo) {
    case 'texto': return t || malo('un texto no vacío');
    case 'eleccion': return t === '' || o.valores.includes(t) ? t : malo(o.valores.join(' o '));
    case 'booleano':
      if (typeof crudo === 'boolean') return crudo;
      if (/^(1|si|sí|true|yes)$/i.test(t)) return true;
      if (/^(0|no|false|)$/i.test(t)) return false;
      return malo('si o no');
    case 'numero': {
      const n = Number(t);
      return t !== '' && Number.isFinite(n) && n >= o.min ? n : malo(`un número mayor o igual a ${o.min}`);
    }
    case 'entero': {
      const n = Number(t);
      return Number.isInteger(n) && n >= o.min && n <= o.max ? n : malo(`un entero entre ${o.min} y ${o.max}`);
    }
    case 'brokers': {
      const lista = t.split(',').map((b) => b.trim()).filter(Boolean);
      const bien = lista.length > 0 && lista.every((b) => /^[A-Za-z0-9.-]+:\d{1,5}$/.test(b));
      return bien ? lista : malo('host:puerto separados por coma, por ejemplo localhost:9092,localhost:9094');
    }
  }
}

function mostrarAyuda() {
  console.log('Uso: node reproducir.mjs [opciones]\n');
  console.log('Cada opción se da en la línea de comandos, como variable de entorno o en un archivo .env.');
  console.log('Gana la línea de comandos, después la variable y al final el valor por defecto.\n');
  for (const o of OPCIONES) {
    console.log(`  --${o.nombre.padEnd(18)} ${o.env.padEnd(22)} por defecto: ${o.defecto || '(vacío)'}`);
    console.log(`  ${' '.repeat(42)}${o.ayuda}`);
  }
  console.log('\nCódigos de salida: 0 todo bien · 1 opciones o dataset · 2 sin conexión o sin topics ·');
  console.log('3 se cayó a mitad · 130 interrumpido con Ctrl+C');
}

function resumenFinal() {
  console.log('Confirmados por el clúster (acks=all):');
  for (const [t, n] of Object.entries(confirmados)) console.log(`  ${t}: ${n}`);
}

async function cerrar() {
  try {
    await producer.close();
  } catch {
    // Si el clúster ya no está, cerrar puede fallar: no cambia nada de lo que se informa.
  }
}

function porTopic(lista) {
  const cuenta = {};
  for (const m of lista) cuenta[m.topic] = (cuenta[m.topic] ?? 0) + 1;
  return Object.entries(cuenta).map(([t, n]) => `${n} en ${t}`).join(', ');
}

function hora(minuto) {
  const m = 21 * 60 + minuto;
  return `${Math.floor(m / 60)}:${String(m % 60).padStart(2, '0')}`;
}

function segundos(ms) {
  return `${(ms / 1000).toFixed(1)} s`;
}
