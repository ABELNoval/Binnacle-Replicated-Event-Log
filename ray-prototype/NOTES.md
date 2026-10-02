# Notas: qué hace Ray por nosotros

Borrador para el mapa de dependencias (Tarea 3) y para el experimento (Tarea 4). Cada entrada dice **dónde** aparece en el código. Lo marcado como *por verificar* es hipótesis hasta medirlo.

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
