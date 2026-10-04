# ray-prototype

Prototipo de la **Fase 1** (Python + Ray Core). Ray hace de andamiaje de middleware para entender el problema antes del diseño nativo.

**Frontera de retirada:** a partir del Hito 2, Ray está **prohibido como dependencia runtime** de la implementación final. Esta carpeta solo se conserva como referencia para la comparación Ray/nativo de la Fase 3. Ningún código en Go puede depender de ella.

## Contenido

| Archivo | Qué es |
|---|---|
| `binnacle_ray/record.py` | `Record` con los mismos campos que `proto/broker.proto` (key, value, timestamp) |
| `binnacle_ray/partition.py` | `PartitionActor`: un actor por partición, con un log ordenado append-only en memoria |
| `binnacle_ray/registry.py` | `TopicRegistry`: mapea cada topic a sus actores de partición; `get_or_create_registry()` lo encuentra por nombre |
| `binnacle_ray/offsets.py` | `OffsetStore`: actor detached con offsets confirmados por `(grupo, topic, partición)`; `commit_offset()` guarda el próximo offset a consumir |
| `binnacle_ray/producer.py` | `Producer`: elige partición por hash SHA-256 de la key y usa round-robin cuando la key está vacía |
| `binnacle_ray/consumer.py` | `Consumer`: asignación estática de particiones, polling desde el offset confirmado y commit tras procesar |
| `binnacle_ray/errors.py` | Errores de negocio del contrato |
| `producer_cli.py` / `consumer_cli.py` | Procesos separados para publicar y consumir por línea de comandos |
| `tests/` | Pruebas con pytest sobre un clúster Ray local |
| `demo_partitions.py` | Crea un topic, escribe en cada partición y muestra en qué proceso vive cada una |
| `demo_producer_consumer.py` | Prueba extremo a extremo: productor y consumidor en procesos separados, reinicio del consumidor y reanudación por offsets confirmados |
| `NOTES.md` | Qué hace Ray por nosotros: insumo de las Tareas 3 y 4 |

## Entorno

Un solo nodo en Windows, con Python 3.13 y Ray 2.58.0 (versiones fijadas en `requirements.txt`). Cada actor corre en su propio proceso worker.

```powershell
cd ray-prototype
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
```

## Cómo probar

Siempre desde `ray-prototype/`, porque los workers de Ray importan `binnacle_ray` desde el directorio de trabajo del driver.

```powershell
.\.venv\Scripts\python -m pytest -v
.\.venv\Scripts\python demo_partitions.py
.\.venv\Scripts\python demo_producer_consumer.py --partitions 3 --messages 12 --first-run 4
```

La demo extremo a extremo crea un topic nuevo, arranca un consumidor, publica desde otro proceso, detiene el consumidor después de `--first-run` mensajes y arranca una segunda instancia con el mismo grupo. La validación exige que no haya duplicados, que los offsets de cada partición sean contiguos y en orden, y que el `OffsetStore` termine con el próximo offset de cada partición.

### Uso manual de los CLI

`producer_cli.py` y `consumer_cli.py` se conectan a un clúster Ray **ya arrancado** (`address="auto"`), y el topic debe existir antes. `demo_partitions.py` se conecta al clúster si está arrancado y crea el topic `demo`:

```powershell
.\.venv\Scripts\ray start --head --include-dashboard=false
.\.venv\Scripts\python demo_partitions.py
.\.venv\Scripts\python producer_cli.py --topic demo --count 10
.\.venv\Scripts\python consumer_cli.py --topic demo --group g1 --idle-timeout-s 2
.\.venv\Scripts\ray stop
```

Si vuelves a lanzar el consumidor con el mismo `--group`, reanuda desde el último offset confirmado. `ray stop` borra todo el estado (topics, logs y offsets), porque vive en memoria.

Semántica de offsets: un offset confirmado es el **próximo registro por consumir**. Por eso, tras procesar el registro en offset `n`, el consumidor confirma `n + 1`. Si un grupo nunca ha confirmado una partición, empieza en `0`. La asignación de particiones es estática (`--partitions 0,2` o todas por defecto); el rebalanceo real queda fuera de este prototipo.