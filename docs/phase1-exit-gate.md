# Puerta de salida de la Fase 1

Respuestas a las cinco preguntas que exige la metodología antes de cruzar la frontera de retirada de Ray. Fecha de cierre: **2026-10-13**.

Fuentes: `docs/contract.md` (contrato observable), `docs/architecture.md` (arquitectura y mapa de dependencias), `experiments/` (experimento inicial) y `docs/decision-log.md` (registro de decisiones).

---

## 1. ¿Qué problema resuelve la aplicación?

Binnacle es un registro de eventos distribuido. Los productores publican eventos en topics divididos en particiones, que son logs ordenados y append-only, y los consumidores los leen a su ritmo, organizados en grupos que se reparten las particiones y guardan su posición (offset). Productores y consumidores no necesitan estar activos al mismo tiempo.

El problema no es almacenar una lista de eventos, sino **mantenerla correcta, ordenada y disponible cuando los procesos y nodos que la sostienen fallan, se reinician o se incorporan**: sin perder eventos confirmados, sin desordenar una partición y permitiendo que cada consumidor retome su lectura donde la dejó.

## 2. ¿Qué comportamiento externo debe sobrevivir al rediseño?

El definido en `docs/contract.md`, que queda **congelado** con esta puerta de salida:

- **Siete operaciones:** crear topic, consultar topics, producir, leer, confirmar offset, consultar el offset confirmado, y unirse o salir de un grupo.
- **Seis errores de negocio:** `TopicAlreadyExists`, `UnknownTopic`, `PartitionOutOfRange`, `InvalidOffset`, `InvalidArgument` y `Unavailable`.
- **Doce garantías (G1–G12):** entre ellas, orden por partición, lotes contiguos, afinidad por key, durabilidad de lo confirmado, reanudación desde el offset confirmado, reintentos sin duplicados, entrega at-least-once, reparto en grupos y tolerancia a la caída de un nodo.
- **Siete historias observables (H1–H7)** que concretan esas garantías y servirán de base para la campaña de la Fase 3.

El diseño nativo puede cambiar toda la arquitectura interna, pero no este comportamiento externo.

## 3. ¿Qué comportamiento viene de nuestro código?

| Comportamiento | Dónde se implementa |
|---|---|
| Elección de partición por hash estable de la key (SHA-256) o round-robin sin key | `Producer.choose_partition` |
| Convención de offsets: el confirmado es el próximo a consumir; un grupo sin commits empieza en 0 | `OffsetStore`, `Consumer` |
| Entrega at-least-once: confirmar cada registro **después** de procesarlo | `Consumer.run` |
| Volver al offset confirmado al salir de `run()`, para no saltarse registros leídos y no procesados | `Consumer.run` (`seek_to_committed`) |
| Asignación estática de particiones a cada consumidor | `Consumer` |
| Validaciones y errores de negocio del contrato | `errors.py`, `TopicRegistry`, `PartitionActor`, `OffsetStore` |
| Sellado del timestamp cuando el productor no lo asigna | `Record.stamped` |

## 4. ¿Qué comportamiento viene de Ray?

| Comportamiento | Mecanismo de Ray |
|---|---|
| Ejecución remota entre procesos sin protocolo propio | Llamadas a métodos de actor (`.remote()`) |
| **Orden y atomicidad dentro de cada partición**, sin ningún lock en nuestro código | Un actor ejecuta un método a la vez |
| Descubrimiento de servicios por nombre | GCS y actores con nombre (`get_if_exists=True`) |
| Ciclo de vida independiente del cliente que crea el servicio | Actores *detached* |
| Decisión de en qué proceso corre cada componente | Scheduling del raylet |
| Transporte de resultados y de errores al llamador | `ObjectRef` y `ray.get`; las excepciones conservan su clase original |
| Detección inmediata de la muerte de un componente, notificada al llamador | `ActorDiedError` (observado en el experimento 1) |
| Eliminación de los actores creados por un componente que muere | Fate-sharing con el dueño (observado en el experimento 2) |
| Distribución del código a los procesos worker | Directorio de trabajo del driver en un clúster local |

La garantía más importante del contrato que hoy se cumple, el orden por partición (G1), **la proporciona Ray y no nuestro código**. En la Fase 2 debe implementarse de forma explícita.

## 5. ¿Qué semántica de fallo observamos en vez de asumir?

Las predicciones se escribieron y se commitearon antes de ejecutar (`bf7cebe`). Se acertaron 8 de 12.

| Fallo provocado | Comportamiento observado | Garantías afectadas |
|---|---|---|
| Crash del proceso de una partición mientras se produce (experimento 1, F1) | Toda llamada a esa partición falla al instante con `ActorDiedError`, sin cuelgues; sus registros confirmados se pierden; Ray no la reinicia; el registro sigue entregando el handle muerto. Las demás particiones siguen funcionando con normalidad | G5 y G11 no se cumplen; G1 se mantiene en las particiones vivas |
| Crash del registro de topics (experimento 2) | Las particiones que creó desaparecen en menos de 1 s (fate-sharing). Un cliente nuevo obtiene un registro nuevo y vacío y recibe `UnknownTopic`: la pérdida es **silenciosa** | El registro es un **punto único de fallo** de datos y metadatos |
| Crash del almacén de offsets y reinicio del consumidor (experimento 3) | Un consumidor nuevo obtiene un almacén vacío, empieza en 0 y reprocesa los 6 registros ya confirmados; el consumidor anterior falla al confirmar | G9 se mantiene; G7 no se cumple |

**Semántica resultante del prototipo:** los fallos se detectan rápido, pero solo los ve quien llama; no hay reintentos ni recuperación; el estado de cualquier componente muere con su proceso; y la caída de un componente de metadatos se presenta a los clientes como ausencia de datos, no como fallo.

**Sin observar todavía:** un crash durante una escritura en curso (relacionado con F8), la caída de un nodo entero y las particiones de red, que no son posibles en un clúster de un solo nodo. Quedan para la campaña de la Fase 3.

---

## Frontera de retirada de Ray

- **El contrato observable queda congelado** a partir de esta fecha. Cualquier cambio posterior se registra como enmienda justificada en `docs/contract.md`.
- **A partir del Hito 2, Ray no puede ser dependencia runtime** de la implementación, ni de forma indirecta mediante un componente que delegue el comportamiento distribuido central.
- `ray-prototype/` se conserva únicamente como referencia para la comparación Ray/nativo de la Fase 3.
- Las preguntas abiertas por el mapa de dependencias se resuelven en el registro de decisiones (`docs/decision-log.md`): discovery, estado, reintento y failover ya tienen diseño elegido; el resto de temas se decide en los Hitos 2 y 3.
