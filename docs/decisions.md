# Decisiones de diseño — Phase 0 (contrato gRPC)

> Documento vivo. Se cierra **antes** de generar código desde `proto/broker.proto`.
> Cualquier cambio aquí obliga a regenerar y avisar al otro.
> Estado: 🟡 En discusión / 🟢 Congelado
> Última actualización: AAAA-MM-DD

---

## Contexto

Este documento responde 7 preguntas que determinan cómo queda `broker.proto`.
Cada decisión incluye **por qué** la tomamos, para que sirva después como
justificación en la documentación final del proyecto.

---

## 1. ¿`key` y `value` son `bytes` o `string`?

**Decisión:** `bytes`.

**Por qué:**
- Kafka permite payloads binarios; no queremos limitar el sistema a texto.
- En Go, `bytes` mapea directo a `[]byte`, que es lo que vamos a escribir
  en el archivo WAL sin conversiones.
- `string` en protobuf fuerza UTF-8 válido y rompe con binario arbitrario.

**Consecuencia:** el productor y el consumidor de prueba (Semana 1) van a
trabajar con `[]byte`; no pasa nada, es lo natural en Go.

---

## 2. ¿Quién pone el `timestamp` del `Record`?

**Decisión:** el productor lo pone; el broker lo respeta tal cual.
Si el productor manda `timestamp = 0`, el broker lo sobrescribe con
`time.Now().UnixMilli()`.

**Por qué:**
- En Semana 1 el productor es nuestro propio cliente, así que controlamos
  el reloj. Ponerlo en el broker complica el caso de replicación (cada
  réplica podría poner uno distinto si se reenvía el mensaje).
- La regla del `0` como "no seteado" evita tener que agregar un campo
  `has_timestamp` después.

**Consecuencia:** documentar esto en el README del proto y en la
documentación final, porque es una decisión de semántica de datos.

---

## 3. ¿El `offset` va dentro del `Record`?

**Decisión:** sí, va como campo `int64 offset` dentro de `Record`, pero:
- En `Produce`, el productor **lo manda en 0 y el broker lo ignora**.
- El broker asigna el offset real al hacer append y lo devuelve en
  `ProduceResponse.offset`.
- En `Fetch`, el `Record.offset` **sí viene seteado** por el broker, para
  que el consumidor sepa exactamente dónde quedó cada mensaje.

**Por qué:**
- El consumidor necesita el offset por cada record (no solo el último)
  para poder hacer commit de su posición parcial si procesa un batch.
- Tenerlo en `Record` evita duplicar el campo en las respuestas.

**Consecuencia:** queda claro que `Record` es la representación del
mensaje *ya almacenado*, no la del mensaje *en tránsito*. Eso es
exactamente como lo modela Kafka.

---

## 4. ¿Cómo se elige la partición de un mensaje?

**Decisión:**
- Si `ProduceRequest.partition >= 0`, el productor fija la partición
  y el broker la respeta.
- Si `ProduceRequest.partition == -1`, el broker hashea
  `Record.key` (`hash(key) % num_partitions`) y decide.
- Si la key viene vacía y `partition == -1`, el broker hace round-robin
  entre las particiones del topic.

**Por qué:**
- Es exactamente el comportamiento de Kafka y lo vamos a poder
  comparar en la documentación final.
- Evita obligar al productor a conocer el número de particiones del
  topic (no hace falta un `ListTopics` solo para publicar).
- El round-robin con key vacía nos deja "orden total dentro de la
  partición, sin orden entre particiones" que pide el PDF (objetivo 2).

**Consecuencia:** el hashing consistente queda del lado del broker en
Semana 1. Si en Semana 2 el productor necesita saber la partición de
antemano (p. ej. para mandar directo al líder), agregamos un RPC
`ListTopics` o un campo en la respuesta. No lo hacemos ahora.

---

## 5. ¿Errores como campo o como gRPC status?

**Decisión:** híbrido.
- **Errores de negocio** (topic no existe, offset inválido, partición
  fuera de rango) → `success = false` + `error = "mensaje"` en la
  respuesta. **No** se devuelve un error gRPC.
- **Errores de protocolo** (payload malformado, campo requerido vacío,
  versión incompatible) → código de estado gRPC (`InvalidArgument`,
  `NotFound`, `Internal`).

**Por qué:**
- Los errores de negocio son parte normal de la operación: un
  consumidor haciendo fetch en un topic que aún no se creó no es un
  fallo de gRPC, es una condición que el cliente debe poder manejar
  sin try/catch.
- Los errores de protocolo sí son excepcionales y es útil que el
  framework de gRPC los propague como `status.Error`.
- Facilita los tests: podemos assertear `success == false` sin
  inspeccionar códigos de gRPC.

**Consecuencia:** definir un pequeño helper en el broker para
construir respuestas de error consistentes.

---

## 6. ¿`Fetch` unario o streaming?

**Decisión:** unario para Semana 1.
Se agrega `rpc FetchStream(FetchRequest) returns (stream Record)`
en Semana 3 **sin romper el contrato** (los RPCs se agregan, no se
modifican).

**Por qué:**
- La Semana 1 es un broker de un solo nodo con `produce` y `fetch`
  básicos. Streaming agrega complejidad que no aporta al hito.
- gRPC permite agregar RPCs después sin tocar los existentes, así
  que no nos estamos casando con nada.
- El fetch continuo (polling) del consumidor se hace desde el
  cliente con un loop; no necesitamos streaming en el transporte
  todavía.

**Consecuencia:** en Semana 3 revisamos si conviene migrar el
consumidor a `FetchStream` para reducir overhead, o si dejamos
polling. Es una decisión reversible.

---

## 7. Nombre del módulo Go y del paquete

**Decisión:**
- Módulo Go: `github.com/<usuario>/distsys-log`
  *(rellenar con el usuario real antes de cerrar esta decisión)*
- Paquete generado: `brokerpb`
- `option go_package = "github.com/<usuario>/distsys-log/gen/brokerpb;brokerpb";`

**Por qué:**
- Tiene que estar fijado **antes** de generar código, porque el
  `go_package` determina la ruta de import en todo el repo.
- `brokerpb` (en vez de `proto` o `pb`) deja claro en los imports
  que es código generado y de qué servicio.

**Consecuencia:** si cambiamos de usuario/organización en GitHub
después, hay que regenerar y actualizar todos los imports. Por eso
lo decidimos ahora.

---

## Resumen ejecutivo

| # | Decisión | Estado |
|---|----------|--------|
| 1 | `key`/`value` son `bytes` | 🟡 |
| 2 | Productor pone timestamp; broker sobrescribe si es 0 | 🟡 |
| 3 | `offset` en `Record`; productor lo ignora, broker lo asigna | 🟡 |
| 4 | `partition = -1` → broker hashea key o hace round-robin | 🟡 |
| 5 | Errores de negocio como campo; protocolo como gRPC status | 🟡 |
| 6 | `Fetch` unario ahora; `FetchStream` en Semana 3 | 🟡 |
| 7 | Módulo `github.com/<usuario>/distsys-log`, paquete `brokerpb` | 🟡 |

**Criterio de cierre:** los 7 quedan en 🟢 y los dos firmamos abajo.

---

## Firmas

- [ ] Compañero A — fecha:
- [ ] Compañero B — fecha:

---

## Cambios posteriores

Cualquier cambio a estas decisiones se agrega abajo con fecha y motivo.
No se edita lo de arriba; se agrega una enmienda.

| Fecha | Campo afectado | Cambio | Motivo |
|-------|----------------|--------|--------|
|       |                |        |        |