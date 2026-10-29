# vidalstore-simulador

Simulador de la noche de la venta de verano de VidalStore, para la EP3 de DSY1107 Desarrollo Cloud
Native I (Duoc UC, 2026-02).

Los launchers de los jugadores, que publicarían la actividad de las partidas, no existen. Este
repositorio los reemplaza: trae una hora de actividad y de compras ya generada, y la publica en el
Kafka del grupo con los mismos tiempos con que ocurrió, con su key y sus headers.

- **La noche:** viernes 6 de noviembre de 2026, de 21:00 a 22:00 (hora de Chile).
- **Lo que se publica:** 3.039 mensajes en `vidalstore.actividad` y 401 en `vidalstore.compras`.
- **Son datos de ejemplo, no reales.** Salen de un generador con semilla fija: el mismo comando da el
  mismo archivo, byte a byte, en cualquier equipo.
- **Trae errores a propósito:** eventos inválidos, duplicados, una partición caliente y una ráfaga de
  partidas en error. Son los que corrige la pauta (ver «Lo que viene mal a propósito»).

Este repositorio **no se copia dentro de los repositorios del grupo**. Se clona al lado de ellos en el
equipo, y en la nube se descarga en `gestion`. El docente usa esta misma copia al corregir.

```text
carpeta-del-grupo/
  vidalstore-plataforma/     compose.yml con Kafka, kafka/topics.sh, levantar.sh
  vidalstore-eventos/
  vidalstore-admin/
  ...
  vidalstore-simulador/      este repositorio
```

## Contenido

| Archivo | Qué es |
|---|---|
| `dataset/actividad.jsonl` | 3.039 mensajes para `vidalstore.actividad`, uno por línea |
| `dataset/compras.jsonl` | 401 mensajes para `vidalstore.compras` |
| `dataset/resumen.json` | Las cifras esperadas y cada anomalía con su `eventoId` y su línea |
| `reproducir.mjs` | Publica el dataset en Kafka respetando los tiempos, y explica cualquier falla |
| `.env.example` | Todas las variables del simulador, comentadas, para copiar como `.env` |
| `generar.mjs` | Vuelve a escribir el dataset; solo hace falta para usar el catálogo del grupo |

El dataset ya viene generado con la semilla 2026 y 30 juegos de ejemplo. **No hace falta correr
`generar.mjs`** para la EP3.

---

# 1. En local

## 1.1 Requisitos

| Qué | Para qué | Comprobar |
|---|---|---|
| Node 24.6 o superior | Correr el simulador | `node -v` |
| Git Bash (Windows) o la terminal (macOS, Linux) | Todos los comandos son de bash | `bash --version` |
| El Kafka del grupo arriba, con los topics creados | Donde se publica | ver 1.3 |

El único paquete es `@platformatic/kafka` 2.14.0, el mismo cliente del curso. Es JavaScript puro, sin
binarios nativos, así que `npm ci` hace lo mismo en Windows, macOS y Linux.

## 1.2 Clonar e instalar

Desde la carpeta donde están los repositorios del grupo:

```bash
git clone https://github.com/Umbingelelo/vidalstore-simulador.git
cd vidalstore-simulador
npm ci
```

## 1.3 Tener Kafka arriba

El simulador no crea topics: el clúster tiene `auto.create.topics.enable=false`. Si falta un topic,
se detiene con `Unknown topic` y el nombre del que falta. Desde `vidalstore-plataforma`:

```bash
docker compose up -d --wait
sh kafka/topics.sh
docker compose ps                    # los 3 controllers y los 3 brokers en (healthy)
```

Por defecto el simulador se conecta a `localhost:9092,localhost:9094,localhost:9096`, los puertos que
publica el `compose.yml` de `ep3/plataforma-kafka` del repositorio del curso. Si el grupo cambió los
puertos, se pasan con `--brokers`.

## 1.4 Publicar la noche

Desde `vidalstore-simulador`:

```bash
node reproducir.mjs                    # la hora en un minuto (--velocidad 60)
node reproducir.mjs --velocidad 600    # en unos 6 segundos
node reproducir.mjs --velocidad 0      # todo de una vez, sin esperas
node reproducir.mjs --solo compras     # solo un topic: actividad o compras
```

`--velocidad 60` es la que se usa para mirar el panel: cada minuto de la noche dura un segundo, y
la ventana de 5 minutos de la alerta se ve en 5 segundos. Al partir muestra con qué valores corre y
de dónde salió cada uno; mientras corre, el avance cada 500 mensajes; y al final, cuántos confirmó el
clúster por topic.

Otras formas útiles (todas las opciones están en la sección 4):

```bash
node reproducir.mjs --seco                           # valida todo sin conectarse a Kafka
node reproducir.mjs --desde 30 --hasta 35            # solo la ráfaga de errores
node reproducir.mjs --sin-invalidos --sin-duplicados # solo eventos buenos
node reproducir.mjs --repetir 5 --velocidad 600      # carga sostenida, para ver el lag
node reproducir.mjs --ayuda                          # todas las opciones
```

## 1.5 Variables y archivo `.env`

Cada opción tiene su variable de entorno con el prefijo `SIM_`: `--velocidad` es `SIM_VELOCIDAD`,
`--brokers` es `SIM_BROKERS`. Para no escribirlas cada vez, se copia `.env.example` como `.env` en la
carpeta del simulador y se descomentan las que se quieran cambiar. Si una opción aparece en más de un
lugar, gana la línea de comandos, después la variable y al final el valor por defecto.

```bash
cp .env.example .env               # y editar, por ejemplo SIM_VELOCIDAD=600
SIM_SOLO=compras node reproducir.mjs   # o una variable solo para esa ejecución
```

`.env` no se versiona (está en `.gitignore`).

## 1.6 Comprobar que llegó todo

Con el clúster recién levantado, los topics vacíos y **una sola** reproducción:

| | `vidalstore.actividad` | `vidalstore.compras` |
|---|---|---|
| Mensajes confirmados | 3.039 | 401 |
| Por partición, P0 · P1 · P2 | 1.751 · 520 · 768 | 147 · 141 · 113 |
| Inválidos a propósito · duplicados | 30 · 15 | 4 · 2 |
| Válidos y únicos | 2.994 | 395: 380 compras y 15 revocaciones |

En Kafka UI (`http://localhost:8085`): **Brokers** muestra tres (ids 4, 5 y 6) y en **Topics** cada
topic tiene esos mensajes. La partición 0 de actividad recibe más del triple que la 1: es el juego
`292030`, con el 40% de la noche.

Desde la terminal, en `vidalstore-plataforma`:

```bash
export MSYS_NO_PATHCONV=1            # solo en Git Bash
docker compose exec -T broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server broker-1:19092 --topic vidalstore.actividad
# vidalstore.actividad:0:1751
# vidalstore.actividad:1:520
# vidalstore.actividad:2:768
```

## 1.7 Volver a empezar

Cada corrida vuelve a publicar todo. Kafka no descarta lo repetido: el log crece y los consumidores
reciben los mismos `eventoId` otra vez. Hay dos formas de repetir una demostración:

- **Partir de cero**, borrando también lo que había en Kafka. En `vidalstore-plataforma`:
  `docker compose down -v`, luego `docker compose up -d --wait` y `sh kafka/topics.sh`. Los offsets
  vuelven a las cifras de 1.6.
- **Sin bajar el clúster**, desde la E2: se vacían las tablas de lectura (`TRUNCATE kpi_minuto,
  alertas, ventas_semana`, más `eventos_procesados` si existe) y se vuelve a reproducir. Los offsets
  se duplican, pero el panel parte de cero porque los consumidores son idempotentes.

## 1.8 Con el catálogo del grupo (opcional)

Con el JSON del catálogo del grupo, los `juegoId` del dataset son los que sirve el microservicio de
catálogo. No resta puntos usar los 30 juegos de ejemplo.

```bash
node generar.mjs --catalogo ../<repo-del-catalogo>/data/juegos.json
```

Reescribe los tres archivos de `dataset/`. Las proporciones se mantienen, pero las cifras exactas
cambian un poco: las nuevas quedan en `dataset/resumen.json`, en `archivos.*.porParticionEsperada`.
El catálogo puede ser un arreglo de ids, un arreglo de objetos con `juegoId`, `id`, `appid`,
`steam_appid` o `slug`, un objeto con el arreglo en `juegos`, `data`, `results`, `items` o `games`, o
un objeto cuyas claves son los ids.

Ese dataset solo existe en el equipo donde se generó. Para usarlo en la nube, ver 2.4.

---

# 2. En la nube (AWS, desde la E2)

En la nube el simulador corre en **`gestion`** (10.0.11.20), en la subred privada de aplicación, que
alcanza a los brokers de `mq-1`, `mq-2` y `mq-3`. No se instala Node: corre dentro de un contenedor
`node:24-slim`. Desde el equipo no se puede publicar en el Kafka de la nube, porque los brokers no
tienen IP pública.

## 2.1 Lo que tiene que estar arriba

- `nat`, para que `gestion` salga a internet a descargar el simulador y la imagen de Node.
- `mq-1`, `mq-2` y `mq-3` con Kafka sanos, y los topics creados desde `mq-1` con
  `sudo SERVICIO=broker BOOTSTRAP=10.0.21.11:9092 sh topics.sh` (E2, paso 8).
- `gestion` con Docker (`docker-user-data.sh` de la E2) y registrada en Session Manager.

## 2.2 Entrar a `gestion` y descargar

Desde la consola de EC2 (*Connect* → *Session Manager*) o desde el equipo con
`aws ssm start-session --target <id-de-gestion>`. Ya adentro:

```bash
cd ~
curl -fsSL https://github.com/Umbingelelo/vidalstore-simulador/archive/refs/heads/main.tar.gz \
  | tar xz
cd vidalstore-simulador-main
```

Se descarga el comprimido en vez de clonar porque la imagen de `gestion` no trae `git`, y porque así
no hace falta ninguna credencial: el repositorio es público.

## 2.3 Publicar en el Kafka de la nube

```bash
sudo docker run --rm -v "$PWD":/app -w /app node:24-slim sh -c \
  "npm ci && node reproducir.mjs --velocidad 60 \
   --brokers 10.0.21.11:9092,10.0.22.12:9092,10.0.21.13:9092"
```

Las demás opciones se pasan igual, al final del comando, o como variables con `-e` antes de
`node:24-slim` (por ejemplo `-e SIM_VELOCIDAD=600`). Un `.env` en `vidalstore-simulador-main` también
sirve, porque la carpeta se monta en el contenedor.

Las tres IP son las de `mq-1`, `mq-2` y `mq-3` de `docs/red.md`; cada broker escucha en el 9092 de su
máquina (`network_mode: host`). Si el grupo usó otras IP, se cambian ahí. La primera vez tarda un
poco más porque descarga la imagen de Node. En el ensayo del curso, desde `gestion`, publicó los
3.440 mensajes en 60 segundos.

## 2.4 Con el dataset generado con el catálogo del grupo

El dataset de 1.8 se sube al bucket del grupo desde el equipo y se baja en `gestion`, encima del que
trae el repositorio:

```bash
# en el equipo, en vidalstore-simulador
aws s3 cp dataset/ s3://<bucket-del-grupo>/simulador/dataset/ --recursive
# en gestion, en vidalstore-simulador-main
aws s3 cp s3://<bucket-del-grupo>/simulador/dataset/ dataset/ --recursive
```

## 2.5 Comprobar desde el equipo

Kafka UI corre en `gestion` y se abre con un túnel de Session Manager, en otro puerto para no chocar
con el 8085 local:

```bash
aws ssm start-session --target <id-de-gestion> \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=8080,localPortNumber=18085"
```

En `http://localhost:18085` tienen que aparecer los tres brokers de la nube y, con los topics vacíos
y una sola reproducción, las mismas cifras por partición de 1.6. Desde `mq-1`, con
`kafka-get-offsets.sh` dentro del contenedor del broker, se ve lo mismo en texto.

## 2.6 En la verificación en vivo y en la defensa

Se lleva `gestion` con el simulador ya descargado (2.2) y `node_modules` instalado, para que el
comando de 2.3 parta de inmediato. En la defensa se usa `--velocidad 60`: el panel se mueve durante un
minuto y la alerta del juego `1328670` aparece entre los segundos 30 y 35, que son las 21:30 a 21:35 de la noche.

---

# 3. Si algo se cae

El simulador no termina con un error del cliente de Kafka sin más. Cada problema se muestra igual:

```text
------------------------------------------------------------------------------
ERROR · Ningún broker acepta la conexión
------------------------------------------------------------------------------
Qué pasó:   En localhost:9092,localhost:9094,localhost:9096 no hay nada escuchando (ECONNREFUSED).
Por qué:    Los brokers se cayeron mientras se publicaba: se detuvo el compose, Docker se cerró o la
            máquina se reinició.
Qué hacer:  1. En vidalstore-plataforma: docker compose up -d --wait
            2. docker compose ps: los brokers en (healthy)
            3. Si el grupo cambió los puertos: --brokers con los suyos
            4. Cuando esté resuelto, seguir desde donde quedó: node reproducir.mjs --desde 10
Detalle técnico:
  PLT_KFK_NETWORK: Connection to localhost:9092 failed.
  ECONNREFUSED: connect ECONNREFUSED 127.0.0.1:9092
------------------------------------------------------------------------------
Código de salida 3 (aMitad).
```

«Qué hacer» se adapta a dónde corre: con brokers en `localhost` da los comandos del equipo, y con IP
privadas da los de la nube. «Detalle técnico» es lo que respondió Kafka, para buscarlo o para llevarlo
a la consulta.

## 3.1 Lo que se recupera solo

Si mientras publica se cae un broker, se pierde la conexión o una partición queda sin líder, el
simulador **no se detiene**: avisa, espera a que el clúster se recupere y vuelve a mandar el lote.

```text
AVISO · El clúster no tiene réplicas suficientes para confirmar
  Los brokers rechazan la escritura: quedan menos de 2 réplicas al día (NOT_ENOUGH_REPLICAS).
  Se espera 10 s a que el clúster se recupere y se vuelve a mandar el lote (intento 1 de 3).
El clúster volvió a confirmar. Se sigue desde 21:11.
```

Lo hace hasta 3 veces, con 10 segundos entre cada una (`--reintentos-lote` y `--espera-lote`). El tiempo
esperando no cuenta: la noche sigue desde donde quedó. Probado con dos brokers detenidos durante 30
segundos a mitad de la hora: el simulador esperó, siguió solo al volver los brokers y terminó con todo
confirmado.

Al volver a mandar un lote, algún mensaje que alcanzó a guardarse puede quedar dos veces en el log (en
esa prueba quedó 1 de 3.039). No es un problema: los consumidores de la E2 lo descartan por `eventoId`.

## 3.2 Lo que no se recupera solo

Si después de esos intentos el clúster sigue sin confirmar, o el problema es de configuración, el
simulador se detiene, muestra cuánto alcanzó a confirmar y **el comando para seguir desde ese minuto**
con `--desde`. Con Ctrl+C pasa lo mismo: termina el lote en curso, cierra el productor y dice desde
dónde seguir. Un segundo Ctrl+C sale de inmediato.

El código de salida dice qué tipo de problema fue, por si se usa en un script:

| Código | Significa | Se publicó algo |
|---|---|---|
| 0 | Todo confirmado por el clúster | Todo |
| 1 | Una opción con un valor que no sirve, o el dataset falta o está dañado | Nada |
| 2 | No se pudo conectar, o faltan los topics | Nada |
| 3 | Se publicó una parte y el clúster dejó de confirmar | Lo que dice el resumen |
| 130 | Interrumpido con Ctrl+C | Lo que dice el resumen |

## 3.3 Los casos que reconoce

| Título del error | Cuándo aparece | Lo primero que hay que revisar |
|---|---|---|
| Falta un topic en el clúster | No se corrió `topics.sh`, o se hizo `docker compose down -v` | `sh kafka/topics.sh` (en la nube, desde `mq-1`) |
| Ningún broker acepta la conexión | El clúster no está arriba, está arrancando o usa otros puertos | `docker compose ps` |
| No se encuentra el nombre de un broker | Se usó `broker-1:19092` desde fuera de Docker | `--brokers localhost:9092,...` en el equipo, IP privadas en la nube |
| Los brokers no responden a tiempo | En la nube, el Security Group de las `mq` o una IP equivocada; en local, Docker sin memoria | La entrada 9092 desde el Security Group de `gestion` |
| El clúster no tiene réplicas suficientes | Quedan menos de 2 brokers vivos | Levantar los brokers caídos. Es lo que debe pasar con `acks=all` |
| Una partición está sin líder | Un broker se cayó y no hay otro elegido todavía | Los 3 controllers y al menos 2 brokers arriba |
| Se cortó la conexión con un broker | Un broker se reinició o la red se cortó | `docker compose logs broker-1 --tail 30` |
| Valor no válido para… | Una opción o variable con un valor que no sirve | El mensaje dice cuál, de dónde salió y qué acepta |
| La línea N de … está dañada | El dataset se editó o la descarga quedó cortada | `git checkout -- dataset/` |
| Error que el simulador no reconoce | Cualquier otro | El detalle técnico, y llevar la salida a la consulta |

## 3.4 Otros problemas

| Lo que aparece | Qué pasa | Qué hacer |
|---|---|---|
| En la nube, `npm ci` no descarga nada | `gestion` no sale a internet | Revisar `nat`, su source/dest check y la ruta de la subred privada |
| Las cifras de Kafka UI son el doble | Se reprodujo dos veces sobre los mismos topics | Es lo esperado: ver 1.7 |
| En Git Bash, `/opt/kafka/...` se convierte en `C:/Program Files/Git/opt/...` | Git Bash traduce rutas | `export MSYS_NO_PATHCONV=1` antes del comando |

---

# 4. Referencia

## Opciones

| `reproducir.mjs` | Variable | Por defecto | Para qué |
|---|---|---|---|
| `--brokers` | `SIM_BROKERS` | `localhost:9092,localhost:9094,localhost:9096` | Bootstrap del clúster |
| `--velocidad` | `SIM_VELOCIDAD` | `60` | Factor sobre el tiempo real. `0` publica sin esperas |
| `--desde` | `SIM_DESDE` | `0` | Minuto de la noche desde el que se publica (0 = 21:00) |
| `--hasta` | `SIM_HASTA` | `60` | Minuto hasta el que se publica, sin incluirlo |
| `--repetir` | `SIM_REPETIR` | `1` | Vueltas seguidas del mismo tramo, para carga sostenida |
| `--solo` | `SIM_SOLO` | los dos | `actividad` o `compras` |
| `--sin-invalidos` | `SIM_SIN_INVALIDOS` | `no` | Omite los 34 inválidos puestos a propósito |
| `--sin-duplicados` | `SIM_SIN_DUPLICADOS` | `no` | Omite los 17 duplicados puestos a propósito |
| `--topic-actividad` | `SIM_TOPIC_ACTIVIDAD` | `vidalstore.actividad` | Otro topic de destino, para pruebas |
| `--topic-compras` | `SIM_TOPIC_COMPRAS` | `vidalstore.compras` | Otro topic de destino, para pruebas |
| `--dataset` | `SIM_DATASET` | `dataset` | Carpeta con los `.jsonl` y `resumen.json` |
| `--cada` | `SIM_CADA` | `500` | Cada cuántos mensajes muestra el avance |
| `--timeout-conexion` | `SIM_TIMEOUT_CONEXION` | `5000` | Milisegundos que se espera a que un broker acepte la conexión |
| `--reintentos` | `SIM_REINTENTOS` | `10` | Reintentos del cliente por envío; cubren la elección de un líder |
| `--espera-reintento` | `SIM_ESPERA_REINTENTO` | `1000` | Milisegundos entre esos reintentos |
| `--reintentos-lote` | `SIM_REINTENTOS_LOTE` | `3` | Veces que se espera al clúster y se vuelve a mandar un lote (3.1) |
| `--espera-lote` | `SIM_ESPERA_LOTE` | `10` | Segundos de cada una de esas esperas |
| `--client-id` | `SIM_CLIENT_ID` | `vidalstore-simulador` | `client.id` del productor |
| `--seco` | `SIM_SECO` | `no` | Valida el dataset y las opciones sin conectarse a Kafka |

Los booleanos aceptan `si`, `sí`, `1`, `true` o `no`, `0`, `false`.

| `generar.mjs` | Por defecto | Para qué |
|---|---|---|
| `--semilla` | `2026` | Otra semilla da otra noche, con las mismas proporciones |
| `--inicio` | `2026-11-06T21:00:00-03:00` | Fecha y hora del minuto 0. `emitidoEn` va en UTC |
| `--catalogo` | lista integrada | JSON con los juegos del grupo |
| `--salida` | `dataset` | Carpeta donde quedan los tres archivos |

El productor es idempotente y usa `acks=all`. Reparte por partición igual que el cliente de Java
(murmur2 de la key), así que la partición de cada key coincide con la de cualquier otro productor
del sistema, incluido el microservicio de licencias.

## Formato de una línea

Cada línea de los `.jsonl` es un mensaje de Kafka completo, con la key, los headers y el value por
separado:

```json
{"desfaseMs":1840512,"key":"292030","headers":{"x-evento-id":"0f8c2a4e-…","x-correlation-id":"7b1e…","content-type":"application/json"},"value":{"eventoId":"0f8c2a4e-…","tipo":"partida.terminada","version":1,"emitidoEn":"2026-11-07T00:30:40.512Z","juegoId":"292030","usuarioSub":"a1b2c3d4-…","datos":{"partidaId":"…","duracionSeg":1840,"resultado":"completada"}}}
```

`desfaseMs` es el momento del evento, en milisegundos desde el minuto 0. No viaja a Kafka: solo le
dice al reproductor cuándo publicar. El value sigue el sobre del punto 7 de la Guía de la EP3, y la
key es `juegoId` en actividad y `usuarioSub` en compras. Todos los eventos de una partida comparten
el `x-correlation-id`.

| Topic | Tipo | `datos` |
|---|---|---|
| `vidalstore.actividad` | `partida.iniciada` | `partidaId`, `plataforma` |
| `vidalstore.actividad` | `partida.terminada` | `partidaId`, `duracionSeg`, `resultado` (`completada`, `abandonada` o `error`) |
| `vidalstore.actividad` | `logro.desbloqueado` | `partidaId`, `logroId` |
| `vidalstore.compras` | `compra.realizada` | `licenciaId`, `adquiridaEn`, `precioBaseClp`, `descuentoPct`, `precioPagadoClp` |
| `vidalstore.compras` | `licencia.revocada` | `licenciaId`, `revocadaPor`, `motivo` |

Las partidas que siguen abiertas a las 22:00 no tienen `partida.terminada`: son las partidas en
curso que el panel tiene que contar.

## Lo que viene mal a propósito

Todo queda anotado en `dataset/resumen.json`, con el `eventoId` y el número de línea. Las cifras son
las de la semilla 2026 con el catálogo de ejemplo.

| Anomalía | Cuánto | Qué ejercita en la EP3 |
|---|---|---|
| Eventos inválidos: JSON cortado, un campo obligatorio faltante y `version: 99` | 30 en actividad y 4 en compras | El desvío al `.dlt` con el motivo en `x-motivo`, sin reintentar (E3) |
| Duplicados exactos: mismo `eventoId` y headers, publicado otra vez entre 0,2 y 30 segundos después | 15 en actividad y 2 en compras | La idempotencia con `UNIQUE (evento_id)` (E2) |
| Un juego con el 40% de la actividad | 40,4% de `vidalstore.actividad` | La partición caliente que provoca la key `juegoId` (defensa) |
| Ráfaga de partidas en error en el juego `1328670`, entre las 21:30 y las 21:35 | 68 de 69 partidas terminadas de ese juego | La regla de `vidalstore-alertas`: más de 20% en error en 5 minutos (E2) |
| Revocaciones | 15, cada una entre 2 y 25 minutos después de la compra | El orden por jugador que da la key `usuarioSub` y el historial (E2) |
| Dos picos de tráfico, a las 21:07 y de 21:44 a 21:46 | de 24 a 83 eventos de actividad por minuto | El lag que sube y baja, y el panel por minuto (E2) |

Fuera de la ráfaga, `resumen.json` lista otras cuatro ventanas de 5 minutos sobre 20%: juegos con una
o dos partidas terminadas, una de ellas en error. La regla de la E2 también las marca. Si el grupo
exige un mínimo de partidas por ventana antes de alertar, lo decide y lo justifica.

## Qué trae `resumen.json`

- `archivos`: por cada `.jsonl`, el topic, la key, el total de líneas, el conteo por tipo y por minuto,
  y `porParticionEsperada`.
- `anomalias.particionCaliente`: el juego, su participación y la partición donde cae.
- `anomalias.rafagaDeErrores` y `anomalias.ventanasDe5MinutosSobre20Pct`: lo que la alerta debería
  detectar, con `minutos` contados desde las 21:00.
- `anomalias.invalidos` y `anomalias.duplicados`: cada uno con su archivo, `eventoId` y líneas.

## Probar con un broker caído

Los dos topics tienen 3 réplicas y `min.insync.replicas=2`, y el productor usa `acks=all`. En local:

```bash
docker compose stop broker-3                          # en vidalstore-plataforma
node reproducir.mjs --solo compras --velocidad 0      # en vidalstore-simulador
```

Con un broker caído, los 401 mensajes se confirman y el ISR de cada partición queda con dos brokers.
Con dos caídos, el clúster responde `NOT_ENOUGH_REPLICAS`: no se confirma nada que viva en una sola
copia. El simulador avisa y espera a que vuelvan (3.1); si no vuelven, se detiene y dice desde dónde
seguir. `docker compose up -d --wait` devuelve los brokers y
el ISR vuelve a tres. En la nube es lo mismo deteniendo Kafka en `mq-2` y `mq-3`.
