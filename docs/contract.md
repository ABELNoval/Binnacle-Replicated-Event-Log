# Contrato observable de Binnacle

> **Estado:** provisional. Se congela al cerrar la puerta de salida de la Fase 1 (#30). Desde ese momento no se edita: cualquier cambio se agrega como enmienda al final, con fecha y motivo.

## Qué es este documento

Es lo que Binnacle le promete a quien lo usa, visto desde fuera. No describe cómo está construido: el mismo contrato vale para el prototipo de la Fase 1 y para el sistema nativo de la Fase 2. Es el invariante entre fases.

Describe el comportamiento del **sistema final**. Lo que cambia de una fase a otra no es el contrato, sino cuánto de él se cumple, y eso se registra en la columna "Fase 1" de la sección de garantías.

Queda fuera de este documento el formato de red (mensajes, codificación, si un error viaja como campo o como código de estado). Eso pertenece a `proto/broker.proto`.

---

## 1. Glosario

| Término | Significado |
|---|---|
| **Topic** | Nombre lógico al que se publican eventos. Tiene un número fijo de particiones, elegido al crearlo. |
| **Partición** | Secuencia ordenada y append-only de registros dentro de un topic. Es la unidad de orden. |
| **Registro** | Un evento: `key` (bytes, puede ser vacía), `value` (bytes) y `timestamp` (milisegundos desde epoch). |
| **Offset** | Posición de un registro dentro de su partición: 0, 1, 2, … sin huecos. Nunca cambia una vez asignado. |
| **Produce confirmado** | Un produce para el que el sistema respondió con éxito y devolvió un offset. |
| **Grupo de consumidores** | Conjunto de consumidores, identificado por nombre, que se reparten las particiones de un topic. |
| **Offset confirmado** | Para un grupo y una partición: el **próximo** offset que el grupo debe consumir. Si el grupo procesó el registro `n`, confirma `n + 1`. |

---

## 2. Operaciones

### 2.1 Crear topic

- **Recibe:** nombre (no vacío), número de particiones (≥ 1) y factor de replicación (≥ 1).
- **Devuelve:** el número de particiones creadas.
- **Errores:** `TopicAlreadyExists` si el nombre ya existe; `InvalidArgument` si algún parámetro no es válido.
- El número de particiones no cambia después de crear el topic.

### 2.2 Consultar topics

- **Recibe:** nada, o el nombre de un topic.
- **Devuelve:** los topics existentes con su número de particiones.
- **Errores:** `UnknownTopic` si se pide un topic que no existe.

### 2.3 Producir

- **Recibe:** topic, uno o más registros y, opcionalmente, la partición destino.
- **Devuelve:** la partición y el offset asignado al primer registro (offset base). Los registros de una misma llamada ocupan offsets consecutivos a partir de ahí.
- **Errores:** `UnknownTopic`; `PartitionOutOfRange` si se indica una partición que no existe; `InvalidArgument` si el lote está vacío o un registro no es válido; `Unavailable` si el sistema no puede confirmar la escritura en ese momento.
- **Elección de partición**, si no se indica:
  - Con key: la misma key va siempre a la misma partición, mientras no cambie el número de particiones del topic.
  - Sin key: los registros se reparten entre las particiones.
- **Timestamp:** si el registro llega sin timestamp (valor 0), el sistema le asigna el momento de la escritura.
- **Si falla con `Unavailable`**, el productor no sabe si la escritura ocurrió y puede reintentar (ver garantía G8).

### 2.4 Leer (fetch)

- **Recibe:** topic, partición, offset inicial y máximo de registros.
- **Devuelve:** registros con su offset, en orden creciente de offset, empezando en el offset pedido. Si el offset está en el final del log o más allá, devuelve una lista vacía.
- **Errores:** `UnknownTopic`; `PartitionOutOfRange`; `InvalidOffset` si el offset es negativo; `InvalidArgument` si el máximo de registros es menor que 1.
- Leer no modifica nada: la misma lectura repetida devuelve los mismos registros.

### 2.5 Confirmar offset

- **Recibe:** grupo, topic, partición y offset (≥ 0).
- **Devuelve:** nada.
- **Errores:** `InvalidOffset` si el offset es negativo; `InvalidArgument` si el grupo o el topic están vacíos.
- Un commit reemplaza al anterior, aunque sea menor: el grupo puede retroceder a propósito para reprocesar. En el sistema final, solo un miembro vigente del grupo puede confirmar offsets de las particiones que tiene asignadas.

### 2.6 Consultar offset confirmado

- **Recibe:** grupo, topic y partición.
- **Devuelve:** el offset confirmado, o `0` si el grupo nunca confirmó esa partición.

### 2.7 Unirse y salir de un grupo

- **Unirse:** un consumidor se une a un grupo para un topic y recibe las particiones que le tocan. El sistema reparte las particiones de modo que cada una tenga como mucho un dueño dentro del grupo.
- **Seguir vivo:** el consumidor debe dar señales de vida periódicamente; si deja de hacerlo, el sistema lo saca del grupo.
- **Salir:** un consumidor puede salir explícitamente.
- **Rebalanceo:** cada vez que alguien entra, sale o deja de dar señales de vida, las particiones se reparten de nuevo. Un consumidor que perdió una partición ya no puede confirmar offsets en ella.

---

## 3. Errores

| Error | Significado |
|---|---|
| `TopicAlreadyExists` | Ya existe un topic con ese nombre. |
| `UnknownTopic` | El topic no existe. |
| `PartitionOutOfRange` | La partición no existe en ese topic, o no está asignada a este consumidor. |
| `InvalidOffset` | El offset no es válido (por ejemplo, negativo). |
| `InvalidArgument` | Falta un dato o tiene un valor no permitido (nombre vacío, lote vacío, número de particiones < 1…). |
| `Unavailable` | El sistema no puede atender la operación ahora (por ejemplo, durante un cambio de líder). Se puede reintentar. |

---

## 4. Garantías

La columna **Fase 1** indica si el prototipo actual ya la cumple. Las demás fases se agregan como columnas nuevas cuando se cierren, sin tocar el texto de la garantía.

| # | Garantía | Fase 1 |
|---|---|---|
| G1 | **Orden por partición:** dentro de una partición, todos los lectores ven los registros en el mismo orden, el orden de los offsets. | Sí |
| G2 | **Lotes contiguos:** los registros de un mismo produce ocupan offsets consecutivos, sin registros de otros productores intercalados. | Sí |
| G3 | **Afinidad por key:** registros con la misma key van a la misma partición, y por tanto se leen en el orden en que se confirmaron. | Sí |
| G4 | **Offsets estables:** un offset, una vez asignado, siempre se refiere al mismo registro. | Sí, mientras el sistema siga en marcha |
| G5 | **Durabilidad:** un produce confirmado no se pierde ante la caída de un broker, según la política de ack declarada. | **No:** los registros viven en memoria y se pierden si cae el proceso que los guarda |
| G6 | **Solo lo confirmado es visible:** un consumidor nunca lee un registro que después pueda desaparecer por un fallo. | No aplica: hay una sola copia de cada registro |
| G7 | **Reanudación:** un consumidor que se reinicia continúa desde el offset confirmado de su grupo. | Sí, si el sistema sigue en marcha. **No** si el sistema entero se reinicia |
| G8 | **Reintentos de produce sin duplicados:** reintentar un produce que sí se escribió no crea un registro duplicado. | **No:** un reintento puede duplicar |
| G9 | **Entrega at-least-once:** si un consumidor confirma después de procesar, todo registro se procesa al menos una vez; un fallo entre procesar y confirmar puede repetirlo. | Sí |
| G10 | **Reparto en grupos:** cada partición tiene como mucho un dueño por grupo, y el reparto se rehace automáticamente al entrar o salir consumidores. | **No:** la asignación es manual y nada impide que dos consumidores del mismo grupo lean la misma partición |
| G11 | **Tolerancia a la caída de un nodo:** si cae el nodo que atiende una partición, otro toma su lugar sin perder registros confirmados. | **No** |
| G12 | **Observabilidad:** se puede consultar el lag de cada grupo, el estado de las réplicas y el líder de cada partición. | **No** |

### Lo que Binnacle no promete

- **Orden entre particiones:** dos registros en particiones distintas no tienen un orden definido entre sí.
- **Exactly-once:** el procesamiento es at-least-once; deduplicar el procesamiento es responsabilidad de quien consume.
- **Cambiar el número de particiones** de un topic existente.
- **Compatibilidad con Kafka** ni con otro sistema: el parecido es de modelo, no de protocolo.

---

## 5. Historias observables

Escenarios concretos que muestran las garantías desde fuera. Sirven como casos de prueba y como base de los experimentos de la Fase 3.

**H1, orden por key (G1, G3).** El productor envía `A` y luego `B`, ambos con key `user-7`, y los dos se confirman. Cualquier consumidor de esa partición lee `A` antes que `B`.

**H2, sin key (G2).** El productor envía 6 registros sin key a un topic de 3 particiones. Se reparten entre las particiones, y dentro de cada partición mantienen el orden en que se confirmaron.

**H3, reinicio del consumidor (G7, G9).** El consumidor del grupo `g1` procesa los offsets 0 a 4 de la partición 0 y confirma `5`. Procesa el 5, pero muere antes de confirmar `6`. Al volver, lee desde el offset 5: el registro 5 se procesa **dos veces**. Es el comportamiento esperado con at-least-once.

**H4, leer más allá del final (2.4).** La partición tiene 3 registros (offsets 0 a 2). Leer desde el offset 3, o desde el 10, devuelve una lista vacía, no un error. Leer desde el −1 falla con `InvalidOffset`.

**H5, respuesta perdida (G8).** El productor envía `A`, el sistema lo escribe en el offset 7, pero la respuesta se pierde y el productor recibe `Unavailable`. El productor reintenta. Con G8 cumplida, `A` aparece una sola vez en el offset 7. Sin G8 (Fase 1), `A` puede aparecer en los offsets 7 y 8.

**H6, caída del líder (G5, G11).** El productor recibe la confirmación de `A` en el offset 12. Cae el nodo que atiende esa partición. Después de la recuperación, cualquier consumidor que lea la partición encuentra `A` en el offset 12.

**H7, rebalanceo (G10).** Los consumidores `c1` y `c2` del grupo `g1` se reparten 4 particiones. `c2` deja de dar señales de vida: sus particiones pasan a `c1`, que continúa desde los offsets confirmados por `c2`. Si `c2` vuelve e intenta confirmar un offset en una partición que perdió, la operación falla con `PartitionOutOfRange`.

---

## 6. Cambios posteriores

Después de congelado, este documento no se edita: cada cambio se agrega aquí.

| Fecha | Sección afectada | Cambio | Motivo |
|---|---|---|---|
| | | | |
