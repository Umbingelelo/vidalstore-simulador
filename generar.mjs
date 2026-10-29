// Genera el dataset de muestra de la EP3: una hora de la venta de verano de VidalStore.
// Datos de ejemplo, no reales. Misma semilla y mismo catálogo, mismo archivo en todos los equipos.
//
//   node generar.mjs [--semilla 2026] [--inicio 2026-11-06T21:00:00-03:00]
//                    [--catalogo data/juegos.json] [--salida dataset]

import { parseArgs } from 'node:util';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const { values: opciones } = parseArgs({
  options: {
    semilla: { type: 'string', default: '2026' },
    inicio: { type: 'string', default: '2026-11-06T21:00:00-03:00' },
    catalogo: { type: 'string' },
    salida: { type: 'string', default: 'dataset' },
  },
});

const semilla = Number.parseInt(opciones.semilla, 10);
if (!Number.isInteger(semilla)) salir(`--semilla debe ser un entero, llegó «${opciones.semilla}»`);
const inicioMs = Date.parse(opciones.inicio);
if (Number.isNaN(inicioMs)) salir(`--inicio no es una fecha ISO 8601 válida: «${opciones.inicio}»`);

const HORA_MS = 3_600_000;
const PARTICIONES = 3;

// Proporciones del dataset. Cambiarlas cambia el archivo para todos: se dejan fijas.
const PARTIDAS = 1300;
const COMPRAS = 380;
const PARTICIPACION_CALIENTE = 0.42; // de las partidas; con logros y ráfaga queda cerca del 40% del topic
const ERROR_BASE = 0.015;
const ERROR_OBJETIVO = 0.08; // de las partidas terminadas, contando la ráfaga
const RAFAGA_DESDE_MS = 30 * 60_000;
const RAFAGA_HASTA_MS = 35 * 60_000;
const PROPORCION_INVALIDOS = 0.01;
const PROPORCION_DUPLICADOS = 0.005;
const PROPORCION_REVOCADAS = 0.04;

// Catálogo integrado: identificadores de ejemplo con forma de appid de Steam. Se reemplaza con
// --catalogo para que los juegoId sean los del catálogo del grupo.
const CATALOGO_EJEMPLO = [
  '1091500', '1245620', '1086940', '730', '570', '271590', '1172470', '578080', '252490', '892970',
  '413150', '105600', '367520', '1145360', '292030', '489830', '1174180', '814380', '1593500', '2050650',
  '1817070', '990080', '2358720', '1716740', '553850', '2379780', '1623730', '1203220', '1328670', '1966720',
];

// ---------- PRNG con semilla (mulberry32) ----------

function mulberry32(a) {
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const azar = mulberry32(semilla);
const entero = (min, max) => min + Math.floor(azar() * (max - min + 1));
const elegir = (lista) => lista[Math.floor(azar() * lista.length)];
function normal() {
  const u = 1 - azar();
  const v = azar();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}
function elegirPonderado(items, pesos, total) {
  let r = azar() * total;
  for (let i = 0; i < items.length; i++) {
    r -= pesos[i];
    if (r < 0) return items[i];
  }
  return items.at(-1);
}

// uuid con forma de v4, sacado del PRNG para que el archivo sea reproducible.
function uuid() {
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i += 4) {
    const n = Math.floor(azar() * 4294967296);
    b[i] = n >>> 24; b[i + 1] = (n >>> 16) & 0xff; b[i + 2] = (n >>> 8) & 0xff; b[i + 3] = n & 0xff;
  }
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = [...b].map((x) => x.toString(16).padStart(2, '0')).join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// ---------- Partición que elige el cliente (murmur2 de Kafka, igual que el cliente de Java) ----------

function murmur2(texto) {
  const data = Buffer.from(texto, 'utf8');
  const largo = data.length;
  const m = 0x5bd1e995;
  let h = 0x9747b28c ^ largo;
  const cuartos = largo >>> 2;
  for (let i = 0; i < cuartos; i++) {
    const j = i * 4;
    let k = data[j] | (data[j + 1] << 8) | (data[j + 2] << 16) | (data[j + 3] << 24);
    k = Math.imul(k, m);
    k ^= k >>> 24;
    k = Math.imul(k, m);
    h = Math.imul(h, m) ^ k;
  }
  const base = largo & ~3;
  switch (largo % 4) {
    case 3: h ^= data[base + 2] << 16; // fall through
    case 2: h ^= data[base + 1] << 8; // fall through
    case 1: h ^= data[base]; h = Math.imul(h, m);
  }
  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;
  return h;
}
const particion = (key) => (murmur2(key) & 0x7fffffff) % PARTICIONES;

// ---------- Catálogo ----------

function leerCatalogo(ruta) {
  if (!ruta) return { origen: 'catálogo de ejemplo integrado', ids: CATALOGO_EJEMPLO };
  let json;
  try {
    json = JSON.parse(readFileSync(ruta, 'utf8'));
  } catch (e) {
    salir(`No se pudo leer el catálogo «${ruta}»: ${e.message}`);
  }
  const campos = ['juegoId', 'id', 'appid', 'appId', 'steam_appid', 'gameID', 'gameId', 'slug'];
  const desdeItem = (item) => {
    if (typeof item === 'string' || typeof item === 'number') return String(item);
    if (item && typeof item === 'object') {
      const campo = campos.find((c) => item[c] !== undefined && item[c] !== null);
      if (campo) return String(item[campo]);
    }
    return null;
  };
  let lista;
  if (Array.isArray(json)) lista = json;
  else if (json && typeof json === 'object') {
    const contenedor = ['juegos', 'data', 'results', 'items', 'games'].find((c) => Array.isArray(json[c]));
    lista = contenedor ? json[contenedor] : Object.keys(json);
  } else lista = [];
  const ids = [...new Set(lista.map(desdeItem).filter(Boolean))];
  if (ids.length < 3) {
    salir(`El catálogo «${ruta}» tiene ${ids.length} juegoId reconocibles; se necesitan al menos 3. ` +
      `Se aceptan un arreglo de ids, un arreglo de objetos con ${campos.join(', ')}, ` +
      'un objeto con un arreglo en juegos, data, results, items o games, o un objeto cuyas claves son los ids.');
  }
  return { origen: resolve(ruta), ids };
}

const catalogo = leerCatalogo(opciones.catalogo);

// Popularidad: un juego caliente, el resto con una cola larga (Zipf). El de la ráfaga de errores es el
// segundo más jugado, para que no se confunda con la partición caliente.
const desordenados = [...catalogo.ids];
for (let i = desordenados.length - 1; i > 0; i--) {
  const j = Math.floor(azar() * (i + 1));
  [desordenados[i], desordenados[j]] = [desordenados[j], desordenados[i]];
}
const juegoCaliente = desordenados[0];
const juegoRafaga = desordenados[1];
const resto = desordenados.slice(1);
const zipf = resto.map((_, i) => 1 / (i + 1) ** 0.9);
const sumaZipf = zipf.reduce((a, b) => a + b, 0);
const juegos = [juegoCaliente, ...resto];
const pesos = [PARTICIPACION_CALIENTE, ...zipf.map((z) => (z / sumaZipf) * (1 - PARTICIPACION_CALIENTE))];
const elegirJuego = () => elegirPonderado(juegos, pesos, 1);

const precios = new Map(juegos.map((id) => [id, {
  base: elegir([4990, 9990, 14990, 19990, 29990, 39990, 49990]),
  descuento: elegir([0, 20, 33, 50, 66, 75]),
}]));

const jugadores = Array.from({ length: 800 }, () => uuid());
const administradores = [uuid(), uuid()];

// ---------- Curva de tráfico: dos picos ----------
// 21:00 abre la venta (pico de entrada) y 21:40 la oferta relámpago (segundo pico).

const campana = (x, centro, ancho) => Math.exp(-((x - centro) ** 2) / (2 * ancho ** 2));
const intensidadActividad = (min) => 0.5 + 1.6 * campana(min, 7, 4) + 1.3 * campana(min, 42, 3.5);
const intensidadCompras = (min) => 0.4 + 2.5 * campana(min, 3, 5) + 1.5 * campana(min, 41, 3);

function instantes(cuantos, intensidad, maximo) {
  const lista = [];
  while (lista.length < cuantos) {
    const t = Math.floor(azar() * HORA_MS);
    if (azar() * maximo <= intensidad(t / 60_000)) lista.push(t);
  }
  return lista.sort((a, b) => a - b);
}

// ---------- Eventos ----------

const emitidoEn = (t) => new Date(inicioMs + t).toISOString();

function linea(t, key, tipo, juegoId, usuarioSub, datos, correlacion) {
  const eventoId = uuid();
  return {
    desfaseMs: t,
    key,
    headers: { 'x-evento-id': eventoId, 'x-correlation-id': correlacion, 'content-type': 'application/json' },
    value: { eventoId, tipo, version: 1, emitidoEn: emitidoEn(t), juegoId, usuarioSub, datos },
  };
}

const actividad = [];
const plataformas = ['windows', 'windows', 'windows', 'windows', 'windows', 'macos', 'linux'];
let terminadas = 0;
let enError = 0;

function partida(inicio, juegoId, usuarioSub, opcionesPartida = {}) {
  const partidaId = uuid();
  const correlacion = uuid();
  actividad.push(linea(inicio, juegoId, 'partida.iniciada', juegoId, usuarioSub,
    { partidaId, plataforma: elegir(plataformas) }, correlacion));
  const duracionSeg = opcionesPartida.duracionSeg
    ?? Math.min(5400, Math.max(60, Math.round(Math.exp(Math.log(900) + 0.8 * normal()))));
  const fin = inicio + duracionSeg * 1000;
  if (!opcionesPartida.resultado) {
    const logros = azar() < 0.6 ? 0 : azar() < 0.75 ? 1 : 2;
    const hasta = Math.min(fin, HORA_MS - 1);
    for (let i = 0; i < logros && hasta > inicio; i++) {
      actividad.push(linea(entero(inicio + 1, hasta), juegoId, 'logro.desbloqueado', juegoId, usuarioSub,
        { partidaId, logroId: `logro-${String(entero(1, 40)).padStart(2, '0')}` }, correlacion));
    }
  }
  if (fin >= HORA_MS) return; // sigue en curso al cerrar la hora
  let resultado = opcionesPartida.resultado;
  if (!resultado) {
    const enRafaga = juegoId === juegoRafaga && fin >= RAFAGA_DESDE_MS && fin < RAFAGA_HASTA_MS;
    const r = azar();
    if (enRafaga) resultado = r < 0.85 ? 'error' : 'abandonada';
    else resultado = r < ERROR_BASE ? 'error' : r < 0.3 ? 'abandonada' : 'completada';
  }
  terminadas++;
  if (resultado === 'error') enError++;
  actividad.push(linea(fin, juegoId, 'partida.terminada', juegoId, usuarioSub,
    { partidaId, duracionSeg, resultado }, correlacion));
}

for (const t of instantes(PARTIDAS, intensidadActividad, 3.2)) partida(t, elegirJuego(), elegir(jugadores));

// Ráfaga: una actualización rota del juego de la ráfaga. Los jugadores reintentan y la partida
// termina en error a los pocos segundos. Se agregan las justas para llegar al ERROR_OBJETIVO.
const reintentos = Math.max(0, Math.ceil((ERROR_OBJETIVO * terminadas - enError) / (1 - ERROR_OBJETIVO)));
const afectados = Array.from({ length: 25 }, () => elegir(jugadores));
for (let i = 0; i < reintentos; i++) {
  const duracionSeg = entero(15, 120);
  const inicio = entero(RAFAGA_DESDE_MS, RAFAGA_HASTA_MS - duracionSeg * 1000 - 1);
  partida(inicio, juegoRafaga, elegir(afectados), { duracionSeg, resultado: 'error' });
}

const compras = [];
const licenciasVendidas = new Set();
const vendidas = [];
for (const t of instantes(COMPRAS, intensidadCompras, 3.0)) {
  let usuarioSub;
  let juegoId;
  do {
    usuarioSub = elegir(jugadores);
    juegoId = elegirJuego();
  } while (licenciasVendidas.has(`${usuarioSub}|${juegoId}`));
  licenciasVendidas.add(`${usuarioSub}|${juegoId}`);
  const { base, descuento } = precios.get(juegoId);
  const licenciaId = uuid();
  const l = linea(t, usuarioSub, 'compra.realizada', juegoId, usuarioSub, {
    licenciaId,
    adquiridaEn: emitidoEn(t),
    precioBaseClp: base,
    descuentoPct: descuento,
    precioPagadoClp: Math.round((base * (100 - descuento)) / 1000) * 10,
  }, uuid());
  compras.push(l);
  vendidas.push(l);
}

// Revocaciones: siempre después de la compra del mismo usuarioSub y juegoId.
const motivos = ['reembolso solicitado', 'cargo desconocido', 'compra duplicada'];
let revocadas = 0;
for (const compra of vendidas.filter(() => azar() < PROPORCION_REVOCADAS)) {
  const t = compra.desfaseMs + entero(2 * 60_000, 25 * 60_000);
  if (t >= HORA_MS) continue;
  const { juegoId, usuarioSub, datos } = compra.value;
  compras.push(linea(t, usuarioSub, 'licencia.revocada', juegoId, usuarioSub, {
    licenciaId: datos.licenciaId,
    revocadaPor: elegir(administradores),
    motivo: elegir(motivos),
  }, uuid()));
  revocadas++;
}

// ---------- Anomalías inyectadas a propósito ----------

const ordenar = (lista) => lista.sort((a, b) => a.desfaseMs - b.desfaseMs);
const camposObligatorios = ['eventoId', 'tipo', 'emitidoEn', 'version'];
const fallas = ['json-roto', 'campo-faltante', 'version-99'];

function inyectar(lista, archivo, registro) {
  const validos = ordenar(lista).slice();
  const invalidos = Math.round(validos.length * PROPORCION_INVALIDOS);
  const duplicados = Math.round(validos.length * PROPORCION_DUPLICADOS);
  for (let i = 0; i < invalidos; i++) {
    const origen = elegir(validos);
    const eventoId = uuid();
    const value = { ...structuredClone(origen.value), eventoId };
    const headers = { ...origen.headers, 'x-evento-id': eventoId };
    const falla = fallas[i % fallas.length];
    let detalle;
    let valor = value;
    if (falla === 'json-roto') {
      const texto = JSON.stringify(value);
      valor = texto.slice(0, Math.floor(texto.length * 0.6));
      detalle = 'el value se corta a la mitad y no parsea';
    } else if (falla === 'campo-faltante') {
      const campo = camposObligatorios[Math.floor(i / fallas.length) % camposObligatorios.length];
      delete value[campo];
      if (campo === 'eventoId') delete headers['x-evento-id'];
      detalle = `falta ${campo}`;
    } else {
      value.version = 99;
      detalle = 'version 99, desconocida para el consumidor';
    }
    lista.push({ desfaseMs: Math.min(HORA_MS - 1, origen.desfaseMs + entero(1, 900)), key: origen.key, headers, value: valor });
    registro.invalidos.push({ archivo, eventoId: headers['x-evento-id'] ?? null, falla, detalle });
  }
  for (let i = 0; i < duplicados; i++) {
    const origen = elegir(validos);
    lista.push({ ...structuredClone(origen), desfaseMs: Math.min(HORA_MS - 1, origen.desfaseMs + entero(200, 30_000)) });
    registro.duplicados.push({ archivo, eventoId: origen.value.eventoId, tipo: origen.value.tipo });
  }
  return ordenar(lista);
}

const anomalias = { invalidos: [], duplicados: [] };
inyectar(actividad, 'actividad.jsonl', anomalias);
inyectar(compras, 'compras.jsonl', anomalias);

// Número de línea (desde 1) de cada anomalía, para buscarla en el archivo.
for (const [archivo, lista] of [['actividad.jsonl', actividad], ['compras.jsonl', compras]]) {
  const lineasPorId = new Map();
  lista.forEach((l, i) => {
    const id = l.headers['x-evento-id'];
    if (id) lineasPorId.set(id, [...(lineasPorId.get(id) ?? []), i + 1]);
  });
  for (const a of [...anomalias.invalidos, ...anomalias.duplicados]) {
    if (a.archivo === archivo && a.eventoId) a.lineas = lineasPorId.get(a.eventoId);
  }
  // El inválido sin eventoId no está en el mapa: se ubica por su contenido.
  lista.forEach((l, i) => {
    if (!l.headers['x-evento-id']) {
      const a = anomalias.invalidos.find((x) => x.archivo === archivo && !x.eventoId && !x.lineas);
      if (a) a.lineas = [i + 1];
    }
  });
}

// ---------- Resumen ----------

function contar(lista) {
  const porTipo = {};
  const porMinuto = Array(60).fill(0);
  const porParticion = Array(PARTICIONES).fill(0);
  for (const l of lista) {
    const tipo = typeof l.value === 'object' ? l.value.tipo ?? '(sin tipo)' : '(json roto)';
    porTipo[tipo] = (porTipo[tipo] ?? 0) + 1;
    porMinuto[Math.floor(l.desfaseMs / 60_000)]++;
    porParticion[particion(l.key)]++;
  }
  return { lineas: lista.length, porTipo, porParticionEsperada: porParticion, porMinuto };
}

function ventanasSobre20() {
  const ventanas = new Map();
  for (const l of actividad) {
    if (typeof l.value !== 'object' || l.value.tipo !== 'partida.terminada' || l.value.version !== 1) continue;
    const v = Math.floor(l.desfaseMs / 300_000);
    const clave = `${l.value.juegoId}|${v}`;
    const c = ventanas.get(clave) ?? { juegoId: l.value.juegoId, ventana: v, terminadas: 0, enError: 0, ids: new Set() };
    if (c.ids.has(l.value.eventoId)) continue; // el duplicado no cuenta dos veces
    c.ids.add(l.value.eventoId);
    c.terminadas++;
    if (l.value.datos?.resultado === 'error') c.enError++;
    ventanas.set(clave, c);
  }
  return [...ventanas.values()]
    .filter((c) => c.enError / c.terminadas > 0.2)
    .sort((a, b) => b.enError - a.enError)
    .map((c) => ({
      juegoId: c.juegoId,
      minutos: `${c.ventana * 5} a ${(c.ventana + 1) * 5}`,
      desde: emitidoEn(c.ventana * 300_000),
      hasta: emitidoEn((c.ventana + 1) * 300_000),
      terminadas: c.terminadas,
      enError: c.enError,
      porcentaje: Math.round((c.enError / c.terminadas) * 1000) / 10,
    }));
}

const deCaliente = actividad.filter((l) => l.key === juegoCaliente).length;
const deRafaga = actividad.filter((l) => typeof l.value === 'object' && l.value.tipo === 'partida.terminada'
  && l.value.version === 1 && l.value.juegoId === juegoRafaga
  && l.desfaseMs >= RAFAGA_DESDE_MS && l.desfaseMs < RAFAGA_HASTA_MS);
const pct = (a, b) => Math.round((a / b) * 1000) / 10;
const idsRafaga = new Set();
const rafagaUnica = deRafaga.filter((l) => !idsRafaga.has(l.value.eventoId) && idsRafaga.add(l.value.eventoId));

const resumen = {
  aviso: 'Datos de ejemplo generados por generar.mjs. No son datos reales de ningún jugador ni de ninguna tienda.',
  generadoCon: {
    semilla,
    inicio: new Date(inicioMs).toISOString(),
    catalogo: catalogo.origen,
    juegosEnCatalogo: catalogo.ids.length,
    particiones: PARTICIONES,
  },
  archivos: {
    'actividad.jsonl': { topic: 'vidalstore.actividad', key: 'juegoId', ...contar(actividad) },
    'compras.jsonl': { topic: 'vidalstore.compras', key: 'usuarioSub', ...contar(compras) },
  },
  anomalias: {
    particionCaliente: {
      juegoId: juegoCaliente,
      eventosEnActividad: deCaliente,
      participacionPct: pct(deCaliente, actividad.length),
      particionEsperada: particion(juegoCaliente),
    },
    rafagaDeErrores: {
      juegoId: juegoRafaga,
      minutos: `${RAFAGA_DESDE_MS / 60_000} a ${RAFAGA_HASTA_MS / 60_000}`,
      desde: emitidoEn(RAFAGA_DESDE_MS),
      hasta: emitidoEn(RAFAGA_HASTA_MS),
      terminadas: rafagaUnica.length,
      enError: rafagaUnica.filter((l) => l.value.datos.resultado === 'error').length,
      porcentaje: pct(rafagaUnica.filter((l) => l.value.datos.resultado === 'error').length, rafagaUnica.length),
    },
    partidasTerminadas: { total: terminadas, enError, porcentaje: pct(enError, terminadas) },
    ventanasDe5MinutosSobre20Pct: ventanasSobre20(),
    revocaciones: revocadas,
    invalidos: anomalias.invalidos,
    duplicados: anomalias.duplicados,
  },
};

mkdirSync(opciones.salida, { recursive: true });
const serializar = (lista) => lista.map(({ desfaseMs, key, headers, value }) =>
  JSON.stringify({ desfaseMs, key, headers, value })).join('\n') + '\n';
writeFileSync(join(opciones.salida, 'actividad.jsonl'), serializar(actividad));
writeFileSync(join(opciones.salida, 'compras.jsonl'), serializar(compras));
writeFileSync(join(opciones.salida, 'resumen.json'), JSON.stringify(resumen, null, 2) + '\n');

console.log(`Semilla ${semilla}, catálogo: ${catalogo.origen} (${catalogo.ids.length} juegos)`);
console.log(`actividad.jsonl: ${actividad.length} líneas · compras.jsonl: ${compras.length} líneas`);
console.log(`Juego caliente ${juegoCaliente}: ${resumen.anomalias.particionCaliente.participacionPct}% de actividad, ` +
  `partición esperada ${resumen.anomalias.particionCaliente.particionEsperada}`);
console.log(`Ráfaga de errores en ${juegoRafaga} entre los minutos 30 y 35: ` +
  `${resumen.anomalias.rafagaDeErrores.enError} de ${resumen.anomalias.rafagaDeErrores.terminadas} partidas en error`);
console.log(`Inválidos: ${anomalias.invalidos.length} · duplicados: ${anomalias.duplicados.length} · ` +
  `revocaciones: ${revocadas}`);
console.log(`Archivos en ${resolve(opciones.salida)}`);

function salir(mensaje) {
  console.error(mensaje);
  process.exit(1);
}
