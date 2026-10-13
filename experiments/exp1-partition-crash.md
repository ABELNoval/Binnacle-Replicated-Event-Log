# Experimento 1 (F1): se cae una partición mientras se produce

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

F1, *single process crash*: muere de forma abrupta el proceso que aloja una partición mientras un productor está escribiendo.

## 2. Workload

Un productor envía registros sin key a un topic de 3 particiones, así que el round-robin los reparte por igual. Primero 30 registros (10 por partición) y, tras el fallo, 30 más con el mismo reparto. Cada `send` espera la confirmación antes del siguiente.

## 3. Afirmación bajo prueba

Si muere el proceso de una partición, se pierde su log y la partición queda inutilizable (hipótesis 1 de `docs/architecture.md`).

## 4. Predicción

Registrada el **2026-10-13, antes de la ejecución**, en el commit `bf7cebe`. Lo que sigue es la redacción formal de esa predicción, sin cambios de contenido; el texto original consta en dicho commit.

- **1.1** Opción (b): los envíos dirigidos a la partición 1 fallan con un error.
- **1.2** Se espera un error propio de Ray, del tipo `RayActorError`.
- **1.3** Los envíos a las demás particiones se completan con normalidad.
- **1.4** La lectura de los registros previos de la partición 1 devuelve un error.
- **1.5** Ray no reinicia la partición, ya que el prototipo no configura `max_restarts` con un valor mayor que 0.
- **1.6** Opción (a): `get_partition(1)` devuelve el mismo actor muerto y su uso falla.

Preguntas a las que responde:

- 1.1 Los `send` a la partición 1 tras el crash: (a) siguen en los offsets 10, 11…; (b) fallan con error enseguida; (c) se quedan colgados; (d) empiezan otra vez en el offset 0.
- 1.2 Tipo de error esperado.
- 1.3 ¿Las particiones 0 y 2 siguen funcionando?
- 1.4 Al leer los 10 registros previos de la partición 1: (a) los devuelve; (b) da error; (c) lista vacía.
- 1.5 ¿Ray revive la partición 1 por su cuenta en 5 segundos?
- 1.6 `get_partition(1)` devuelve: (a) el mismo actor muerto, y usarlo falla; (b) un actor nuevo que funciona; (c) un error del registro.

## 5. Configuración

- Un nodo de Ray en Windows 11, Python 3.13.0, Ray 2.58.0 (`ray-prototype/requirements.txt`).
- Código del prototipo sin modificar: `TopicRegistry`, `PartitionActor` y `Producer`. Las particiones se crean sin opciones, con los valores por defecto de Ray.
- Topic `exp1` con 3 particiones.
- Una operación que tarda más de 10 s cuenta como "colgada".
- Script: `ray-prototype/fault_experiments.py`.

## 6. Trigger del fallo

Después de los 30 primeros registros se obtiene el PID del proceso de la partición 1 con `whereami()` y se mata con `taskkill /F /PID <pid>`: un crash abrupto del proceso, no un cierre ordenado.

## 7. Evidencia recogida

Ejecutado el 2026-10-13 a las 08:49 (hora local), después del commit `bf7cebe` que contiene la predicción.

- `experiments/evidence/exp1.jsonl`: 73 líneas, una por operación, con su resultado o error, la clase del error y el tiempo.
- `experiments/evidence/console.log`: aviso del raylet sobre el worker 18780, `Worker exit type: SYSTEM_ERROR`, "connection error code 10054".
- Ubicación: partición 0 en el PID 28376, partición 1 en el 18780 y partición 2 en el 32532, todas en el mismo nodo.

## 8. Resultado observado

| Paso | Observado |
|---|---|
| 30 envíos antes del fallo | 30 de 30 confirmados, offsets 0 a 9 en cada partición (máximo 0,019 s por envío) |
| `taskkill` del PID 18780 | Código 0; el proceso deja de existir |
| 10 envíos a la partición 1 después | **Los 10 fallan al instante** (1–2 ms) con `ActorDiedError`: *"The actor died unexpectedly before finishing this task"*. Ninguno se cuelga |
| 20 envíos a las particiones 0 y 2 | Todos confirmados; continúan en los offsets 10 a 19 |
| Leer la partición 1 desde el offset 0 | `ActorDiedError`: los 10 registros confirmados quedan inaccesibles |
| `get_partition(1)` en el registro | Devuelve el **mismo** actor (mismo `actor_id`); usarlo da `ActorDiedError` |
| A los 5 s | El proceso sigue muerto; `next_offset` y `append` dan `ActorDiedError`. Ray no la reinicia |

La clase del error es `ActorDiedError`, que hereda de `RayActorError` y esta de `RayError`.

**Comparación con la predicción: 6 de 6.**

| Pregunta | Predicción | Observado | |
|---|---|---|---|
| 1.1 | (b) fallan con error | Fallan al instante | ✅ |
| 1.2 | Error de Ray, `RayActorError` | `ActorDiedError`, subclase de `RayActorError` | ✅ (la clase exacta es más específica) |
| 1.3 | Las otras particiones funcionan | Offsets 10 a 19 sin problemas | ✅ |
| 1.4 | Da error | `ActorDiedError` | ✅ |
| 1.5 | No la revive | Sigue muerta a los 5 s | ✅ |
| 1.6 | (a) el mismo actor muerto | Mismo `actor_id`; usarlo falla | ✅ |

## 9. Conclusión

Se **confirma** la afirmación. Con los valores por defecto (`max_restarts=0`), la muerte del proceso de una partición:

1. **Pierde sus datos confirmados.** Los 10 registros vivían solo en la memoria de ese proceso: el prototipo incumple G5 (durabilidad) y G11 (tolerancia a la caída de un nodo).
2. **Queda aislada.** Las otras particiones siguen confirmando con sus offsets correctos: el fallo no se propaga y G1 se mantiene en las particiones vivas.
3. **Se detecta al instante, pero solo por quien llama.** Ray avisa a cada llamador con `ActorDiedError` en 1–2 ms, sin cuelgues. Nuestro código no detecta nada por su cuenta: el registro sigue entregando el handle muerto como si la partición existiera.

**Para la Fase 2:** la durabilidad necesita un WAL en disco y réplicas (Tareas 7 y 8); los metadatos deben reflejar la caída (Tarea 9). Además, ese aviso instantáneo es un privilegio de estar en la misma máquina: Ray ve cerrarse la conexión local. Entre nodos por red, un nodo caído y uno lento se ven igual, así que la detección tendrá que basarse en heartbeats y timeouts.

## 10. Incertidumbre restante

- Un solo nodo: no se probó la caída de un nodo entero ni la detección a través de la red.
- No se provocó el crash **durante** un `append` en vuelo, así que sigue abierto si el productor puede saber si ese lote se escribió (relacionado con F8).
- No se probó `max_restarts > 0` (revivir el actor sin recuperar su estado).
- Una sola ejecución, en Windows y con `taskkill`; la detección inmediata puede depender de cómo el sistema operativo cierra el socket.
