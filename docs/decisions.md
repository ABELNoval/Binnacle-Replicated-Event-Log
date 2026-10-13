# Decisiones de diseño del contrato gRPC (Fase 0)

> **Estado:** en discusión. Las decisiones se cierran en el issue #22, junto con su conciliación con `proto/broker.proto`, **antes** de generar código a partir del proto. Cualquier cambio posterior obliga a regenerar el código.

## Contexto

Este documento responde a siete preguntas que determinan la forma de `proto/broker.proto`. Cada decisión incluye su justificación, para que sirva como fundamento en la documentación final del proyecto.

| # | Decisión | Estado |
|---|---|---|
| 1 | `key` y `value` son `bytes` | En discusión |
| 2 | El productor asigna el timestamp; el broker lo sobrescribe si vale 0 | En discusión |
| 3 | `offset` dentro de `Record`: el productor no lo usa y el broker lo asigna | En discusión |
| 4 | `partition = -1`: el broker aplica hash a la key o round-robin | En discusión |
| 5 | Errores de negocio como campo de la respuesta; errores de protocolo como código de estado gRPC | En discusión |
| 6 | `Fetch` unario; `FetchStream` como ampliación posterior | En discusión |
| 7 | Módulo Go `github.com/<usuario>/distsys-log` y paquete `brokerpb` | En discusión |

---

## 1. Tipo de `key` y `value`

**Decisión:** `bytes`.

**Justificación:**
- Los payloads pueden ser binarios; limitar el sistema a texto restringiría su uso.
- En Go, `bytes` se corresponde directamente con `[]byte`, que es lo que se escribe en el WAL sin conversiones.
- `string` en Protocol Buffers exige UTF-8 válido, lo que impide transportar datos binarios arbitrarios.

**Consecuencia:** el productor y el consumidor trabajan con `[]byte`, la representación natural en Go.

## 2. Asignación del `timestamp` de `Record`

**Decisión:** el productor asigna el timestamp y el broker lo respeta. Si el productor envía `timestamp = 0`, el broker lo sustituye por `time.Now().UnixMilli()`.

**Justificación:**
- Asignarlo en el broker complica la replicación: cada réplica podría asignar un valor distinto si el mensaje se reenvía.
- Usar `0` como "no asignado" evita añadir más adelante un campo `has_timestamp`.

**Consecuencia:** la regla debe documentarse junto al proto y en la documentación final, por tratarse de una decisión sobre la semántica de los datos.

## 3. Ubicación del `offset`

**Decisión:** el offset es un campo `int64 offset` dentro de `Record`, con estas reglas:
- En `Produce`, el productor lo envía en 0 y el broker lo ignora.
- El broker asigna el offset real al añadir el registro y lo devuelve en `ProduceResponse.offset`.
- En `Fetch`, el broker sí lo informa en cada `Record`, para que el consumidor conozca la posición exacta de cada mensaje.

**Justificación:**
- El consumidor necesita el offset de cada registro, no solo el del último, para confirmar posiciones parciales al procesar un lote.
- Incluirlo en `Record` evita duplicar el campo en las respuestas.

**Consecuencia:** `Record` representa el mensaje *ya almacenado*, no el mensaje *en tránsito*, del mismo modo que lo modela Kafka.

## 4. Elección de la partición

**Decisión:**
- Si `ProduceRequest.partition >= 0`, el productor fija la partición y el broker la respeta.
- Si `ProduceRequest.partition == -1`, el broker aplica un hash a `Record.key` (`hash(key) % num_partitions`).
- Si la key está vacía y `partition == -1`, el broker reparte por round-robin entre las particiones del topic.

**Justificación:**
- Reproduce el comportamiento de Kafka, lo que facilita la comparación en la documentación final.
- El productor no necesita conocer el número de particiones del topic, así que no hace falta una consulta `ListTopics` solo para publicar.
- El round-robin con key vacía mantiene el orden total dentro de cada partición, sin orden entre particiones (objetivo 2 del enunciado P13).

**Consecuencia:** el hashing queda del lado del broker en el sistema de un solo nodo. Si más adelante el productor necesita conocer la partición de antemano (por ejemplo, para enviar directamente al líder), se añadirá una consulta de metadatos o un campo en la respuesta.

## 5. Representación de los errores

**Decisión:** modelo híbrido.
- **Errores de negocio** (topic inexistente, offset inválido, partición fuera de rango): `success = false` y `error = "mensaje"` en la respuesta, sin error gRPC.
- **Errores de protocolo** (payload malformado, campo obligatorio vacío, versión incompatible): código de estado gRPC (`InvalidArgument`, `NotFound`, `Internal`).

**Justificación:**
- Los errores de negocio forman parte del funcionamiento normal: leer de un topic que aún no existe es una condición que el cliente debe poder tratar sin manejar excepciones.
- Los errores de protocolo son excepcionales, y conviene que gRPC los propague como `status.Error`.
- Las pruebas pueden comprobar `success == false` sin inspeccionar códigos gRPC.

**Consecuencia:** el broker necesita una función auxiliar que construya respuestas de error de forma uniforme.

## 6. `Fetch` unario o en streaming

**Decisión:** `Fetch` unario para el broker de un solo nodo. Más adelante puede añadirse `rpc FetchStream(FetchRequest) returns (stream Record)` sin romper el contrato, ya que los RPC se añaden y no se modifican.

**Justificación:**
- Para un broker de un solo nodo con `Produce` y `Fetch` básicos, el streaming añade complejidad sin aportar al hito.
- gRPC permite añadir RPC sin modificar los existentes, por lo que la decisión es reversible.
- La lectura continua se resuelve en el cliente con un bucle de polling.

**Consecuencia:** al implementar los grupos de consumidores se evaluará si conviene migrar el consumidor a `FetchStream` para reducir la sobrecarga o mantener el polling.

## 7. Módulo Go y paquete generado

**Decisión:**
- Módulo Go: `github.com/<usuario>/distsys-log` (pendiente de fijar la ruta definitiva antes de cerrar esta decisión).
- Paquete generado: `brokerpb`.
- `option go_package = "github.com/<usuario>/distsys-log/gen/brokerpb;brokerpb";`

**Justificación:**
- La ruta debe fijarse **antes** de generar código, porque `go_package` determina la ruta de importación en todo el repositorio.
- `brokerpb` (en lugar de `proto` o `pb`) deja claro en los imports que se trata de código generado y a qué servicio pertenece.

**Consecuencia:** un cambio posterior de usuario u organización en GitHub obligaría a regenerar el código y actualizar todos los imports.

---

## Cambios posteriores

Cada cambio a estas decisiones se añade aquí con fecha y motivo. El texto anterior no se edita: se añade una enmienda.

| Fecha | Sección afectada | Cambio | Motivo |
|---|---|---|---|
| | | | |
