# Registro de decisiones

Demuestra que las abstracciones de Ray se retiran **de forma deliberada** al pasar al diseño nativo, y no se sustituyen por una colección arbitraria de tecnologías. Se inicia al cerrar la Fase 1 (2026-10-13) y se completa durante la Fase 2.

Formato de la metodología: *Requisito | Fase 1 Ray | Alternativas nativas | Diseño elegido | Razón / compromiso*. La evidencia citada está en `experiments/` y en `docs/architecture.md`.

---

## 1. Retirada de las abstracciones de Ray

| Requisito | Fase 1 Ray | Alternativas nativas | Diseño elegido | Razón / compromiso |
|---|---|---|---|---|
| **discovery** | Runtime: el GCS registra los actores con nombre y los clientes los encuentran con `ray.init(address="auto")` | Estático / registro / gossip / DHT | **Lista estática de brokers semilla + consulta de metadatos al clúster** | El clúster es pequeño y fijo (3 brokers en Docker Compose); gossip y DHT resuelven clústeres grandes y cambiantes. El experimento 2 mostró que un registro en un único proceso es un punto único de fallo y que el descubrimiento por nombre esconde las caídas, así que los metadatos no pueden depender de un solo proceso. *Compromiso:* añadir un broker exige cambiar la configuración. |
| **estado** | Actor: log de cada partición en la memoria de su proceso | Disco local / primary-backup / log replicado / CRDT | **Log replicado líder → réplicas, con WAL en disco local en cada broker** | El experimento 1 mostró que el estado en memoria se pierde con el proceso. Solo disco local no sobrevive a la caída de un nodo. CRDT no encaja: el orden total por partición exige un único escritor. *Compromiso:* cada escritura confirmada cuesta, como mínimo, una escritura en disco y la espera de las réplicas que exija la política de ack. |
| **reintento** | Runtime: con los valores por defecto no hay reinicio ni reintento; el error llega al llamador | Idempotencia / dedup / no reintentar | **El productor reintenta y el broker deduplica por ID de productor + número de secuencia; el consumidor mantiene at-least-once** | Sin reintentos, un fallo transitorio pierde la escritura; con reintentos sin deduplicación, una respuesta perdida (F8) duplica el registro. La deduplicación permite cumplir G8. *Compromiso:* el broker debe guardar, y replicar, la última secuencia de cada productor. |
| **failover** | Runtime: ninguno; un actor muerto no se reinicia (`max_restarts=0`) | Reinicio / elección / reencaminamiento | **Elección de un nuevo líder entre las réplicas sincronizadas, con epochs** | Reiniciar solo recupera lo que esté en disco y deja la partición sin servicio mientras tanto. Reencaminar requiere otra copia de los datos, que la replicación ya proporciona. Elegir solo entre réplicas sincronizadas protege los registros confirmados; los epochs evitan dos líderes simultáneos. *Compromiso:* la detección por timeout puede dar falsos positivos y provocar elecciones innecesarias. |

---

## 2. Decisiones del contrato observable

Fijadas en `docs/contract.md` y congeladas con él.

| Decisión | Razón / compromiso |
|---|---|
| El offset confirmado es el **próximo** registro a consumir | Un consumidor que confirma `n + 1` tras procesar `n` reanuda sin ambigüedad; un grupo sin commits empieza en 0. |
| Un commit puede retroceder, pero solo lo hace un miembro vigente del grupo | Permite reprocesar a propósito. Restringirlo a miembros vigentes evita que un consumidor obsoleto haga retroceder al grupo. |
| Exactly-once queda fuera de alcance; la entrega es **at-least-once** | Confirmar después de procesar garantiza que ningún registro se pierde (G9). *Compromiso:* un fallo entre procesar y confirmar repite el registro; deduplicar el procesamiento es responsabilidad del consumidor. |
| El número de particiones de un topic es fijo | Cambiarlo alteraría la partición de destino de cada key y rompería la afinidad por key (G3). |
| La partición se elige por un hash estable de la key, o por round-robin si la key está vacía | Un hash estable (SHA-256 en el prototipo) envía la misma key a la misma partición desde cualquier proceso; el `hash()` de Python no es estable entre procesos. |
| La Fase 1 se ejecuta en un solo nodo de Ray en Windows | El soporte multi-nodo de Ray en Windows es limitado. Cada actor corre en su propio proceso, sin memoria compartida. *Compromiso:* no se observaron la caída de un nodo entero ni las particiones de red. |

---

## 3. Temas pendientes para la Fase 2

Temas de la metodología que todavía no tienen diseño elegido. Se deciden durante los Hitos 2 y 3, y los que se omitan deben justificarse.

| Tema | Pregunta clave | Dónde se decide |
|---|---|---|
| Comunicación | ¿Qué protocolo conecta los procesos? (gRPC propuesto en `proto/broker.proto`) | #22 |
| Identidad | ¿Cómo se identifican brokers, productores, consumidores y operaciones? | Hito 2 |
| Membresía | ¿Cómo se representan los brokers y los miembros de cada grupo? | Hitos 2 y 3 |
| Propiedad del estado | ¿Qué nodo es autoritativo sobre cada partición y sobre cada grupo? | Hitos 2 y 3 |
| Durabilidad | ¿Cuándo se hace `fsync` y cuándo es honesto confirmar un produce? | Hito 2 |
| Replicación | Conjunto de réplicas sincronizadas, high watermark y política de ack | Hito 2 |
| Consistencia | ¿Qué historias pueden observar los clientes? (solo lo confirmado, G6) | Hito 2 |
| Detección de fallos | Heartbeats, timeouts y su justificación | Hito 2 |
| Partición de red | ¿Qué lado puede seguir aceptando escrituras? | Hito 2 |
| Recuperación y reconciliación | ¿Qué hace un broker al volver y cómo repara un log divergente? | Hito 2 |
| Scheduling | Asignación de particiones a brokers y a consumidores | Hitos 2 y 3 |
| Backpressure | Límites de lote, de peticiones en vuelo y de cola | Hito 3 |
| Observabilidad | Métricas (lag, líder, réplicas) y logs estructurados con IDs estables | Hito 3 |
| Seguridad | Modelo de confianza explícito | Hito 3 |

---

## Cambios posteriores

| Fecha | Decisión afectada | Cambio | Motivo |
|---|---|---|---|
| | | | |
