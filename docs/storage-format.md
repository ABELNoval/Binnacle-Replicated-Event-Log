# Formato de almacenamiento en disco — diseño

> Documento de diseño. **No es implementación.** Se cierra antes de tocar
> `internal/storage`.
> Estado: 🟡 En discusión / 🟢 Congelado
> Issue: Joint — Native design groundwork
> Última actualización: AAAA-MM-DD

---

## 1. Alcance

Este documento define:

- El layout binario de cada `Record` en el archivo `.log`.
- El layout de directorios por partición.
- La convención de nombres de segmentos e índices.
- Cómo se reconstruye el índice en memoria al arrancar el broker.
- Las opciones de política de `fsync` y su trade-off durabilidad/rendimiento.
- Qué se pierde en un corte de energía bajo cada opción.

**Fuera de alcance (Semana 2+):** replicación, elección de líder, commit
de offsets de consumidor. El formato que definimos acá debe sobrevivir
esos cambios, pero no los resuelve.

---

## 2. Principios de diseño

1. **Append-only.** Solo se escribe al final del segmento activo. Nunca
   se modifica un byte ya escrito.
2. **Auto-contenido.** Cada record incluye todo lo necesario para
   parsearlo sin leer los anteriores. Esto permite reconstruir el
   índice escaneando desde cualquier punto.
3. **Detectable la corrupción.** Cada record lleva CRC propio. Un byte
   corrupto invalida *ese* record, no el archivo entero.
4. **Truncable.** Si al arrancar encontramos un record inválido (CRC
   malo, length absurdo), truncamos el archivo ahí y seguimos. Esto
   maneja el caso de escritura a medias por corte de energía.
5. **Versionable.** Cada record lleva `magic` + `version`. Cambiar el
   formato en el futuro no rompe archivos viejos.
6. **Little-endian.** x86/ARM nativos, coincide con
   `binary.LittleEndian` en Go. No hay razón para big-endian en este
   proyecto.

---

## 3. Formato binario del Record

### 3.1 Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ offset  size  field        tipo        descripción               │
├──────────────────────────────────────────────────────────────────┤
│  0       4    length       uint32      bytes que siguen a este   │
│                                        campo (header + body)     │
│  4       4    crc32c       uint32      CRC32-Castagnoli sobre    │
│                                        bytes [8 .. 8+length-4)   │
│  8       1    magic        uint8       = 0x4C ('L' de Log)       │
│  9       1    version      uint8       = 0x01                    │
│ 10       2    flags        uint16      reservado, 0 por ahora    │
│ 12       8    offset       int64       posición lógica en la     │
│                                        partición (asignada por   │
│                                        el broker)                 │
│ 20       8    timestamp    int64       epoch millis              │
│ 28       4    key_len      int32       -1 = null, 0 = vacío,     │
│                                        N > 0 = N bytes           │
│ 32       4    value_len    int32       >= 0                      │
│ 36       K    key          [K bytes]   K = max(key_len, 0)       │
│ 36+K     V    value        [V bytes]   V = value_len             │
└──────────────────────────────────────────────────────────────────┘

  K = key_len   (si key_len == -1, K = 0)
  V = value_len
  length = 4 (crc) + 1 + 1 + 2 + 8 + 8 + 4 + 4 + K + V
         = 32 + K + V
```

**Overhead fijo por record:** 36 bytes (4 de `length` + 32 de header).
Para un mensaje con key de 7 bytes y value de 13 bytes, son 56 bytes
totales.

### 3.2 Decisiones y por qué

- **`length` primero, no `offset` primero.** El parser necesita saber
  cuántos bytes leer *antes* de poder interpretar nada. Poner `offset`
  primero obligaría a leer 8 bytes fijos antes de saber si el record
  está completo.
- **`crc32c` (Castagnoli), no `crc32` (IEEE).** Es el que usa Kafka,
  tiene mejor detección de errores en hardware moderno (instrucción
  `SSE4.2` en x86, `CRC32` en ARMv8), y Go lo trae en
  `hash/crc32.MakeTable(crc32.Castagnoli)`.
- **CRC cubre desde `magic` hasta el final del `value`, NO cubre
  `length`.** Si `length` se corrompe, no queremos que el CRC dé
  "válido" sobre una longitud falsa. El `length` se valida aparte
  (ver §3.3).
- **`magic` + `version` separados.** `magic` detecta "esto no es un
  record mío"; `version` permite evolucionar el formato. Cuesta 2
  bytes y ahorra un dolor de cabeza futuro.
- **`key_len = -1` para null key.** Distinto de `key_len = 0` (key
  vacía). Kafka hace esta distinción y la vamos a necesitar en
  Semana 3 cuando el particionado dependa de si hay key o no.
- **`timestamp` dentro del record, no en un archivo aparte.** El
  consumidor lo necesita al hacer `Fetch`. Tenerlo en el record evita
  un segundo lookup.

### 3.3 Límites y validación

- **`max_record_size = 1 MiB` (1 048 576 bytes).** Si `length` > este
  valor, consideramos el record inválido y truncamos el archivo ahí.
  Esto evita que un `length` corrupto nos haga leer gigabytes.
- **`length >= 32`.** El header mínimo (sin key ni value) son 32 bytes.
  Si `length < 32`, el record está corrupto.
- **`key_len` y `value_len` dentro de rango.** `key_len` en
  `[-1, max_record_size]`, `value_len` en `[0, max_record_size]`.
- **CRC mismatch → record inválido.**

Si cualquiera de estas validaciones falla **en el último record del
archivo**, asumimos escritura a medias por corte de energía y truncamos
ahí. Si falla **en un record intermedio**, el archivo está corrupto y el
broker no debe arrancar (o debe arrancar en modo degradado y avisar).

### 3.4 Ejemplo byte a byte

Supongamos:

- `offset = 1`
- `timestamp = 1700000000000` ms (= `0x0000018BCFE56800`)
- `key = "user-42"` (7 bytes)
- `value = "bought:widget"` (13 bytes)
- `flags = 0`, `magic = 0x4C`, `version = 0x01`

Cálculo previo:

```
K = 7
V = 13
length = 32 + 7 + 13 = 52 (0x00000034)
```

Bytes en disco (little-endian):

```
offset  hex                                             ascii              campo
------  ----------------------------------------------  -----------------  -------------------
 0      34 00 00 00                                                        length = 52
 4      ?? ?? ?? ??                                                        crc32c (placeholder)
 8      4C                                                                 magic = 'L'
 9      01                                                                 version = 1
10      00 00                                                              flags = 0
12      01 00 00 00 00 00 00 00                                            offset = 1
20      00 68 E5 FC 8B 01 00 00                                            timestamp = 1700000000000
28      07 00 00 00                                                        key_len = 7
32      0D 00 00 00                                                        value_len = 13
36      75 73 65 72 2D 34 32                             "user-42"          key
43      62 6F 75 67 68 74 3A 77 69 64 67 65 74            "bought:widget"    value
56      --                                                                 fin del record
```

Verificación del `timestamp`:
```
1700000000000 decimal
= 0x0000018BCFE56800 hex
= 00 68 E5 FC 8B 01 00 00  (little-endian)
```

Verificación del tamaño total:
```
4 (length) + 52 (length payload) = 56 bytes
```

El siguiente record empieza en el byte 56 del segmento.

**El CRC no se puede precalcular a mano aquí, pero en la implementación
será:**
```
crc = crc32.Checksum(bytes[8:56], crc32.MakeTable(crc32.Castagnoli))
```
Es decir, cubre `magic` hasta el último byte de `value`, que son
`56 - 8 = 48` bytes.

---

## 4. Layout de directorios

### 4.1 Estructura completa

```
data/
└── broker-<id>/                            # un dir por instancia de broker
    └── <topic>-<partition>/                # un dir por partición
        ├── 00000000000000000000.log        # segmento base offset 0
        ├── 00000000000000000000.index      # índice sparse del segmento
        ├── 00000000000000000000.timeindex  # (opcional, Semana 3+)
        ├── 00000000000000100000.log        # segmento base offset 100000
        ├── 00000000000000100000.index
        └── ...
```

**Ejemplo concreto:**
```
data/broker-1/orders-0/00000000000000000000.log
data/broker-1/orders-0/00000000000000000000.index
data/broker-1/orders-1/00000000000000000000.log
data/broker-2/orders-0/00000000000000000000.log
```

### 4.2 Por qué `broker-<id>/` al principio

- Cada broker corre en su propio contenedor y monta
  `data/broker-<id>/` como volumen. Si corren dos brokers en la misma
  máquina de desarrollo, no se pisan.
- El `<id>` es el mismo que usa el broker para identificarse en el
  clúster (Semana 2).

### 4.3 Por qué `<topic>-<partition>/` y no `<topic>/<partition>/`

- Evita anidamiento innecesario (`ls data/` muestra todo plano).
- El nombre codifica la clave de partición en un solo string, lo que
  simplifica operaciones como "borrar la partición 3 de `orders`".
- Los topics y particiones no van a tener `/` en el nombre (se valida
  al crear el topic).

### 4.4 Convención de nombres de segmento

- **20 dígitos, zero-padded, en decimal:** `00000000000000000000.log`.
- El número es el **offset base** del segmento (el offset del primer
  record que contiene).
- El primer segmento siempre es `...00000.log` (base 0).
- Cuando el segmento activo supera `segment.max.bytes` (propuesto:
  64 MiB para el proyecto, ajustable), se hace `close()` y se crea uno
  nuevo cuyo nombre es el offset del próximo record a escribir.

**Por qué 20 dígitos:**
- `int64` máximo tiene 19 dígitos; 20 da margen.
- Ancho fijo → orden lexicográfico = orden numérico. Esto permite
  listar segmentos con `os.ReadDir` y quedan ordenados sin parsear.
- Misma convención que Kafka; facilita la comparación en la memoria
  final.

### 4.5 Archivos de índice (`.index`)

- Formato: pares `(offset:int64, position:int64)` consecutivos, sin
  header, sin CRC. Cada entrada son 16 bytes.
- **Sparse**: no guardamos una entrada por record, solo cada
  `index.interval.bytes` (propuesto: 4096 bytes). Esto reduce el
  tamaño del índice ~100x a costa de escanear hasta 4 KiB al hacer
  un `fetch` que cae entre dos entradas.
- Se reconstruye al arrancar si falta o está corrupto (ver §5).

**Para Semana 1 podemos omitir el `.index`** y hacer solo escaneo
secuencial. Lo agregamos cuando midamos que el fetch por offset se
vuelve lento. La estructura de directorios ya lo contempla.

### 4.6 `.timeindex` (diferido)

Kafka mantiene un índice `timestamp → offset` para búsquedas por
tiempo. No lo necesitamos para los objetivos del PDF. Lo dejamos
anotado como "Semana 3+ si sobra tiempo".

---

## 5. Reconstrucción del índice en memoria

### 5.1 Estructuras en memoria

Por partición, el broker mantiene:

```go
type Partition struct {
    dir         string
    segments    []*Segment       // ordenados por baseOffset
    active      *Segment         // el último, abierto para escritura
    nextOffset  int64            // próximo offset a asignar
}

type Segment struct {
    baseOffset  int64
    logPath     string
    logFile     *os.File
    size        int64            // bytes escritos
    index       map[int64]int64  // offset -> byte position en logFile
    indexSorted []int64          // offsets ordenados (para binary search)
}
```

- `index` es `map[int64]int64` para lookup O(1) durante `fetch`.
- `indexSorted` permite encontrar el offset más cercano menor o igual
  cuando el `fetch` pide un offset que no está indexado (búsqueda
  binaria + scan desde ahí).
- Si el `.index` está en disco, se carga directo. Si no, se reconstruye
  escaneando el `.log`.

### 5.2 Algoritmo de reconstrucción

Al arrancar el broker, para cada partición:

```
1. Listar todos los archivos *.log en el directorio de la partición.
2. Ordenar por nombre (que es por baseOffset, por el zero-padding).
3. Para cada segmento, en orden:
   a. position = 0
   b. loop:
        i.   Leer 4 bytes de length en `position`.
        ii.  Si EOF limpio (0 bytes leídos) → fin del segmento, ok.
        iii. Si quedan < 4 bytes → escritura a medias, truncar en
             `position`, fin.
        iv.  Si length < 32 o length > max_record_size → inválido,
             truncar en `position`, fin.
        v.   Leer `length` bytes desde position+4.
        vi.  Si no se pueden leer todos → escritura a medias,
             truncar en `position`, fin.
        vii. Verificar magic == 0x4C y version == 0x01.
        viii.Verificar CRC32C sobre los bytes leídos.
        ix.  Si algo falla en (vii) o (viii) → truncar en `position`,
             fin (asumiendo último record; ver §5.3).
        x.   Parsear header, obtener offset.
        xi.  Si (position - lastIndexedPos) >= index.interval_bytes:
                agregar (offset, position) al índice.
        xii. position += 4 + length
   c. Si el segmento se truncó, borrar el `.index` correspondiente
      (se regenera en el próximo flush).
4. El último segmento (el de mayor baseOffset) queda abierto para
   escritura. `nextOffset` = último offset parseado + 1.
```

### 5.3 Manejo de corrupción

- **Corrupción en el último record del último segmento:** es el caso
  normal de corte de energía. Truncamos y arrancamos. El broker puede
  loggear un warning con cuántos bytes se perdieron.
- **Corrupción en un record intermedio:** el archivo está corrupto y
  no podemos garantizar orden. El broker debe:
  - Opción A (estricta): negarse a arrancar esa partición y loggear
    error. La réplica (Semana 2) toma el liderazgo.
  - Opción B (laxa): truncar desde el record corrupto hasta el final
    del segmento, perdiendo datos. **Solo aceptable si la partición
    no tenía datos confirmados más allá de ese punto**, lo cual es
    difícil de saber sin replicación.
  - **Recomendación:** Opción A. En Semana 1 (single-node) es
    aceptable que el broker no arranque; el operador decide qué hacer.

---

## 6. Políticas de fsync

### 6.1 Por qué importa

`write(2)` en Linux escribe a la **page cache** del kernel, no al
disco. Los datos sobreviven a la muerte del **proceso** broker (la
page cache es del kernel), pero **no** a un corte de energía o kernel
panic. Para que sobrevivan a un corte de energía hace falta `fsync(2)`,
que fuerza el flush al dispositivo.

**Consecuencia clave para el diseño de acks:** si el broker responde
`ProduceResponse{success: true}` *antes* de hacer `fsync`, está
mintiendo. Ese ack no garantiza durabilidad ante corte de energía.

La política de fsync y la política de ack están acopladas. Este
documento define la primera; la segunda se cierra en Semana 2.

### 6.2 Opciones

| Política | Cuándo se hace fsync | Throughput aprox. | Durabilidad ante corte de energía |
|----------|----------------------|-------------------|-----------------------------------|
| **Per-record** | Después de cada `append`, antes del ack | ~100–1 000 rec/s | Ningún record ackeado se pierde |
| **Per-batch** | Al cerrar un batch (por tiempo o tamaño), antes de los acks del batch | ~50 000–500 000 rec/s | Ningún record ackeado se pierde (los no ackeados del batch en curso sí) |
| **Periodic** | Cada `fsync.interval.ms` (ej. 100 ms) o `fsync.interval.records` (ej. 10 000), lo que pase primero | Cercano al máximo del disco | Hasta `interval` de records ackeados se pierden |
| **None** | Nunca explícito, confía en el writeback del kernel (~30 s por defecto) | Máximo | Hasta ~30 s de records ackeados se pierden |

### 6.3 Qué se pierde exactamente en un corte de energía

Esta es la parte que ambos deben poder explicar. Distinguimos tres
escenarios:

**Escenario A — el proceso broker muere (kill -9, OOM, panic).**
El kernel sigue vivo. La page cache se preserva. **Ninguna política
pierde datos** ya escritos con `write()`, ackeado o no. Esto es
importante: "matar el contenedor" en las pruebas de la Semana 2 **no
es** un corte de energía.

**Escenario B — la máquina pierde energía (o kernel panic).**
La page cache se pierde. Lo que no se haya `fsync`eado, se va.

- **Per-record:** se pierde como máximo el último record en vuelo, que
  aún no fue ackeado. Los ackeos previos son verdad. **Cero pérdida
  de datos confirmados.**
- **Per-batch:** se pierde el batch en curso (los records que aún no
  recibieron ack). Los batches anteriores son verdad. **Cero pérdida
  de datos confirmados**, asumiendo que el ack se emite *después* del
  fsync del batch. Si el ack se emite antes, se pierde el batch
  completo.
- **Periodic (100 ms):** se pierden todos los records ackeado en los
  últimos ~100 ms. En una carga de 100 000 rec/s, son ~10 000 records
  perdidos. **Pérdida de datos confirmados.** El cliente no se enteró.
- **None:** se pierden hasta ~30 s de records ackeado (dirty page
  writeback del kernel). En la misma carga, ~3 000 000 records.
  **Pérdida masiva de datos confirmados.**

**Escenario C — falla el disco (sector corrupto, disco muerto).**
Ninguna política de fsync ayuda. La única defensa es replicación
(Semana 2). Este escenario justifica por qué el PDF pide réplicas.

### 6.4 Implicación para la política de ack (Semana 2)

Cuando definamos la política de ack, la regla será:

- `acks=0`: el productor no espera confirmación. Da igual el fsync.
  Semántica: at-most-once.
- `acks=1`: el broker responde cuando el líder tiene el record. Si
  usamos fsync per-record o per-batch, el ack implica durabilidad
  local. Si usamos periodic/none, el ack es una promesa débil.
- `acks=all`: el broker responde cuando el líder **y** las réplicas
  mínimas tienen el record. La durabilidad viene de la replicación,
  no del fsync local. Es el modelo de Kafka: `acks=all` sin fsync
  por record sigue siendo durable porque hay N copias en N máquinas.

**Conclusión de diseño:** la política de fsync óptima depende de si
hay replicación.

- **Semana 1 (single-node):** para que las pruebas de durabilidad del
  PDF ("sobrevive a un reinicio del broker") sean honestas, conviene
  `per-batch` con un batch pequeño (ej. 5 ms o 100 records). Da
  durabilidad real sin matar el throughput. `per-record` funciona pero
  va a ser doloroso de demostrar con volumen.
- **Semana 2+ (replicado):** se puede relajar a `periodic` porque la
  replicación provee la durabilidad. Documentar explícitamente que el
  fsync local ya no es la garantía principal.

### 6.5 Decisión para Semana 1

**Propuesta:** `per-batch` con `fsync.batch.ms = 5` y
`fsync.batch.records = 100` (lo que ocurra primero). El broker emite
los acks del batch **después** del fsync.

**Estado:** 🟡 abierto hasta el cierre del Milestone 1. Si la
implementación de batching agrega demasiada complejidad, fallback a
`per-record` y documentar la degradación de throughput.

---

## 7. Preguntas abiertas

| # | Pregunta | Dueño | Fecha límite |
|---|----------|-------|--------------|
| 1 | ¿`segment.max.bytes` = 64 MiB es razonable para la demo? | ambos | antes de Milestone 1 |
| 2 | ¿Hacemos `.index` en Semana 1 o lo diferimos? | ambos | antes de Milestone 1 |
| 3 | ¿`fsync.batch` se implementa con goroutine + timer o con un canal con timeout? | ambos | durante Milestone 1 |
| 4 | ¿Qué hacemos si hay corrupción en record intermedio (Opción A vs B de §5.3)? | ambos | antes de Milestone 1 |
| 5 | ¿El `timestamp` lo escribe el broker o lo respeta del productor? (ver `decisions.md` §2) | ambos | cerrado en `decisions.md` |

---

## 8. Criterios de "done" de este issue

- [ ] Ambos miembros pueden dibujar el layout del record en un
      pizarrón sin mirar este documento.
- [ ] Ambos miembros pueden explicar qué se pierde en un corte de
      energía bajo cada una de las 4 políticas de fsync, y por qué
      "matar el proceso" ≠ "corte de energía".
- [ ] Este documento está commiteado en `docs/storage-format.md`.
- [ ] El issue se cierra solo cuando los dos firman abajo.

**Firmas:**
- [ ] Compañero A — fecha:
- [ ] Compañero B — fecha:

---

## 9. Referencias

- Apache Kafka, *Log compaction* y *Message format* (para comparación
  en la memoria final): https://kafka.apache.org/documentation/#implementation
- `hash/crc32` de Go, tabla Castagnoli:
  https://pkg.go.dev/hash/crc32#MakeTable
- `binary.LittleEndian`:
  https://pkg.go.dev/encoding/binary#LittleEndian
- `fsync(2)`: https://man7.org/linux/man-pages/man2/fsync.2.html