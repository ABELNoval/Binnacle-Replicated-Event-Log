# Notas: qué hace Ray por nosotros

Borrador para el mapa de dependencias (Tarea 3) y para el experimento (Tarea 4). Cada entrada dice **dónde** aparece en el código. Lo marcado como *por verificar* es hipótesis hasta medirlo.

## Particiones y registro de topics (#25)

| Requisito | Qué lo provee hoy (Ray) | Dónde se ve | Pregunta para el diseño nativo |
|---|---|---|---|
| Orden y atomicidad por partición | Un actor ejecuta un método a la vez (`max_concurrency=1` por defecto). No escribimos ningún lock. | `PartitionActor.append`; `test_concurrent_writers_get_contiguous_gap_free_offsets` | ¿Qué mecanismo (mutex, goroutine dueña del log) da la misma garantía en Go? |
| Ejecución remota | `@ray.remote` + `.remote()`: cada actor vive en su propio proceso worker. | `whereami()`; `test_partitions_run_in_separate_processes`; `demo_partitions.py` | ¿Qué protocolo explícito (gRPC) reemplaza la llamada a un método de actor? |
| Resultado remoto | `ObjectRef` + `ray.get` (bloquea hasta tener el resultado). | Todas las llamadas | ¿Basta con una respuesta con offset + ack? ¿Qué pasa si la respuesta se pierde? |
| Errores de negocio | Ray reenvía la excepción del actor al llamador, y sigue siendo de la clase original (`except UnknownTopic` funciona). | `errors.py`; tests de `pytest.raises` | Hay que definir los errores en el contrato gRPC (¿campo o status code?). |
| Descubrimiento | Los actores con nombre se registran en el GCS de Ray; `get_if_exists=True` busca o crea. | `get_or_create_registry()` | ¿Lista estática de semillas + RPC de metadatos? |
| Ciclo de vida | `lifetime="detached"`: el registro sobrevive al driver que lo creó. | `get_or_create_registry()` | ¿Quién arranca y mantiene vivo cada broker? |
| Propiedad del estado | El registro crea las particiones, así que es su **dueño**: si el registro muere, Ray mata también las particiones (fate-sharing). *Por verificar.* | `TopicRegistry.create_topic` | ¿Qué proceso es autoritativo para cada partición? |
| Fallo de un actor | `max_restarts=0` por defecto: si el proceso de una partición muere, la partición no vuelve y su log en memoria se pierde. *Por verificar en la Tarea 4.* | `PartitionActor` (sin opciones) | ¿WAL en disco + recuperación al reiniciar? |
| Distribución del código | Los workers importan `binnacle_ray` sin configurar nada: Ray les pasa el directorio de trabajo del driver en un clúster local. | Ejecutar desde `ray-prototype/` | No aplica en nativo (un binario por proceso), pero explica por qué hay que lanzar desde esta carpeta. |
| Sellado de tiempo | Lo hace nuestro código: si `timestamp == 0`, la partición lo sella (regla de `docs/decisions.md`). | `Record.stamped()` | Se mantiene igual. |

## Productor, consumidor y offsets (#26)

| Requisito | Qué lo provee hoy | Dónde se ve | Pregunta para el diseño nativo |
|---|---|---|---|
| Unirse al sistema (bootstrap) | `ray.init(address="auto")` encuentra el clúster local y, a través del GCS, los actores con nombre. Productor y consumidor no conocen ninguna dirección de red. | `producer_cli.py`, `consumer_cli.py` | ¿Lista estática de brokers semilla? ¿Cómo encuentra un cliente al líder de cada partición? |
| Elección de partición | **Nuestro código:** SHA-256 de la key (estable entre procesos, a diferencia de `hash()` de Python) o round-robin si la key está vacía. | `Producer.choose_partition`; `test_key_hash_is_stable_across_producer_instances` | Se mantiene igual: la regla vive en el cliente o en el broker (decisión del contrato). |
| Ack de escritura | El `ray.get` del `append` devuelve el offset asignado. Eso es todo el "ack": no hay política ni réplicas. | `Producer.send` | ¿Cuándo es un mensaje "seguro"? Política de ack líder vs. líder + réplicas. |
| Reintentos del productor | Ninguno explícito. Si el `append` falla, el error llega al productor y el mensaje no se reenvía. *Por verificar: si Ray reintenta por su cuenta una llamada a actor.* | `Producer.send` | ¿Reintentar? Si sí: ID de productor + secuencia para deduplicar. |
| Estado de los offsets | Actor `OffsetStore` detached y con nombre, **en memoria**. Sobrevive al reinicio del consumidor, pero **no** a `ray stop` ni a la caída del actor. | `offsets.py`; `demo_producer_consumer.py` | Persistencia real (SQLite o log en disco) para que "offset recuperable" sea cierto. |
| Monotonía del commit | Ninguna: un commit puede **retroceder** el offset del grupo. Es intencional (como en Kafka), pero un consumidor obsoleto podría hacer retroceder al grupo. | `test_commit_overwrites_previous_offset` | ¿Rechazar commits de una generación vieja (generation ID)? |
| Semántica de entrega | **Nuestro código:** `run()` confirma `offset + 1` **después** del handler, así que es at-least-once. `commit_current_positions()` tras `poll()` sin procesar sería at-most-once. | `Consumer.run`, `Consumer.commit_current_positions` | Declararla en el contrato y demostrarla con F8/F9. |
| Coordinación del grupo | Ninguna: la asignación es estática. Nada impide que dos consumidores del mismo grupo lean la misma partición y se pisen los commits. | `Consumer(partitions=...)` | Coordinador de grupo, heartbeats y rebalanceo (Fase 2). |
| Coste por mensaje | Dos llamadas remotas por mensaje: un `read` (por lote) y un `commit_offset` por registro. | `Consumer.run` | Commit por lote o periódico, y medirlo en la comparación Ray/nativo. |

## Hallazgo de la revisión del #26: posición local vs. offset confirmado

El consumidor tiene dos "punteros" por partición: la **posición local** (hasta dónde leyó) y el **offset confirmado** (hasta dónde procesó). `poll()` adelanta la posición local de todas las particiones antes de que el handler procese los registros.

- **Fallo observado:** si `run()` terminaba antes de procesar todo lo leído (por llegar a `max_messages` o porque el handler lanzaba una excepción), esa misma instancia se saltaba los registros pendientes en su siguiente `run()`. Con 3 particiones de 4 registros y `max_messages=4`, la instancia solo llegó a procesar 4 de 12. Los registros no se perdían para siempre, porque al no estar confirmados una instancia **nueva** sí los recibía; por eso la demo, que usa un proceso por consumidor, no lo detectaba.
- **Corrección:** `run()` termina siempre con `seek_to_committed()` en un `finally`. Como cada registro procesado se confirma justo después, lo confirmado coincide con lo procesado, y la posición local vuelve a ese punto.
- **Evidencia:** `test_same_instance_does_not_skip_records_after_early_stop` y `test_same_instance_retries_record_after_handler_error` fallan sin la corrección y pasan con ella.
- **Lección para la Fase 2:** "leído" y "procesado" son estados distintos. El consumidor nativo debe tratar la posición de fetch como desechable y el offset confirmado como la única verdad para retomar.
