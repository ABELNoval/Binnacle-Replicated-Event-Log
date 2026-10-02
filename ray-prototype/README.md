# ray-prototype

Prototipo de la **Fase 1** (Python + Ray Core). Ray hace de andamiaje de middleware para entender el problema antes del diseño nativo.

**Frontera de retirada:** a partir del Hito 2, Ray está **prohibido como dependencia runtime** de la implementación final. Esta carpeta solo se conserva como referencia para la comparación Ray/nativo de la Fase 3. Ningún código en Go puede depender de ella.

## Contenido

| Archivo | Qué es |
|---|---|
| `binnacle_ray/record.py` | `Record` con los mismos campos que `proto/broker.proto` (key, value, timestamp) |
| `binnacle_ray/partition.py` | `PartitionActor`: un actor por partición, con un log ordenado append-only en memoria |
| `binnacle_ray/registry.py` | `TopicRegistry`: mapea cada topic a sus actores de partición; `get_or_create_registry()` lo encuentra por nombre |
| `binnacle_ray/errors.py` | Errores de negocio del contrato |
| `tests/` | Pruebas con pytest sobre un clúster Ray local |
| `demo_partitions.py` | Crea un topic, escribe en cada partición y muestra en qué proceso vive cada una |
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
```
