# Experimento 2: se cae el dueño de las particiones (registro de topics)

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

F1, *single process crash*, aplicado al proceso que **creó** las particiones: el registro de topics.

## 2. Workload

Un topic de 3 particiones con 5 registros en cada una. Después del fallo: lecturas de las tres particiones con los handles obtenidos antes del crash, y un cliente nuevo (`Producer`) que busca el topic desde cero.

## 3. Afirmación bajo prueba

Si muere el registro de topics, mueren también las particiones que creó, porque es su dueño (hipótesis 2 de `docs/architecture.md`).

## 4. Predicción

Registrada el **2026-10-13, antes de la ejecución**, en el commit `bf7cebe`. Lo que sigue es la redacción formal de esa predicción, sin cambios de contenido; el texto original consta en dicho commit.

- **2.1** Opción (a): las particiones siguen vivas, ya que, al tratarse de actores *detached*, seguirían existiendo aunque se termine el proceso del registro.
- **2.2** Opción (a): el nuevo `Producer("exp2")` funciona con normalidad, por dos motivos: el topic sigue existiendo y puede localizarse; y, aunque no existiera, se crearía, al tratarse de una operación `get_or_create`.
- **2.3** Sí: el topic sigue existiendo, ya que el cliente accede a él mediante una petición `get_or_create`.

Preguntas a las que responde:

- 2.1 Las particiones tras morir el registro: (a) siguen vivas y se pueden leer con los handles guardados; (b) mueren con él; (c) sus procesos siguen vivos, pero no se pueden usar.
- 2.2 Un `Producer("exp2")` nuevo: (a) funciona normal; (b) falla con `UnknownTopic`; (c) falla con un error de Ray (actor muerto); (d) se cuelga.
- 2.3 Para un cliente nuevo, ¿el topic `exp2` sigue existiendo?

## 5. Configuración

- Un nodo de Ray en Windows 11, Python 3.13.0, Ray 2.58.0 (`ray-prototype/requirements.txt`).
- Código del prototipo sin modificar. El registro se obtiene con `get_or_create_registry()`: actor con nombre, *detached*, `get_if_exists=True`. Las particiones las crea el registro dentro de `create_topic`, sin opciones.
- Topic `exp2` con 3 particiones.
- Una operación que tarda más de 10 s cuenta como "colgada". Comprobaciones a 1 s y a 5 s del crash.
- Script: `ray-prototype/fault_experiments.py`.

## 6. Trigger del fallo

Se obtiene el PID del proceso del registro (ejecutando `os.getpid()` dentro del actor, sin modificar su clase) y se mata con `taskkill /F /PID <pid>`.

## 7. Evidencia recogida

Ejecutado el 2026-10-13 a las 08:49 (hora local), después del commit `bf7cebe` que contiene la predicción.

- `experiments/evidence/exp2.jsonl`: 18 líneas.
- `experiments/evidence/console.log`: el raylet informa **solo** de la muerte del worker 12576 (el registro) como `SYSTEM_ERROR`. No informa de las particiones, aunque también murieron.
- Ubicación: registro en el PID 12576; particiones en los PID 928, 24952 y 20600, todas en el mismo nodo.

## 8. Resultado observado

| Paso | Observado |
|---|---|
| `taskkill` del registro (PID 12576) | Código 0; el proceso deja de existir |
| Procesos de las particiones al segundo | **Los 3 han desaparecido** (928, 24952 y 20600) |
| Leer con los handles guardados (a 1 s y a 5 s) | Las 3 particiones dan `ActorDiedError` |
| `Producer("exp2")` nuevo | Falla en 0,057 s con `RayTaskError(UnknownTopic)`, lanzado por un `TopicRegistry` **nuevo** (PID 32036) |
| ¿`get_or_create_registry()` devuelve el mismo actor? | No: es un registro nuevo |
| `list_topics()` del registro nuevo | `{}`: está vacío |

**Comparación con la predicción: 0 de 3.**

| Pregunta | Predicción | Observado | |
|---|---|---|---|
| 2.1 | (a) siguen vivas, porque es *detached* | (b) mueren con el registro en menos de 1 s | ❌ |
| 2.2 | (a) funciona: el topic existe, y si no, `get_or_create` lo crearía | (b) `UnknownTopic` desde un registro nuevo y vacío | ❌ |
| 2.3 | Sí, sigue existiendo | No: un cliente nuevo no lo encuentra | ❌ |

**Análisis de las predicciones incorrectas:**

- *Detached* significa que el actor **sobrevive a que termine el driver que lo creó**, no que sobreviva a la muerte de su propio proceso. Además, las particiones **no** eran *detached*: las creó el registro, así que el registro era su dueño, y Ray elimina a los actores de un dueño que muere (*fate-sharing*). Por eso el raylet no informa de ellas como errores: no se cayeron, las cerró Ray.
- `get_or_create_registry()` crea un **registro**, no un **topic**. Como el registro muerto liberó su nombre, se creó uno nuevo y vacío, y el topic `exp2` ya no está en ninguna parte.

## 9. Conclusión

Se **confirma** la afirmación. El registro de topics es un **punto único de fallo** de todo el prototipo: al morir arrastra a todas las particiones que creó (*fate-sharing*) y se pierden a la vez los metadatos (qué topics existen) y los datos (los registros).

Lo más grave es que **el fallo es silencioso** para un cliente nuevo. No ve un error de "sistema caído", sino `UnknownTopic`, un error de negocio que dice que el topic nunca existió. Un productor podría volver a crear el topic y empezar de cero sin que nadie se entere de la pérdida.

**Para la Fase 2:** la vida de una partición no puede depender del proceso que guarda los metadatos. Los metadatos de topics deben ser persistentes y replicados, y un cliente debe poder distinguir "este topic no existe" de "el servicio de metadatos perdió su estado".

## 10. Incertidumbre restante

- No se midió con precisión cuánto tarda Ray en eliminar las particiones; solo que en menos de 1 s ya no estaban.
- No se probó qué pasaría si las particiones fueran *detached* (sobrevivirían, pero nadie sabría dónde están: el registro nuevo no las conoce).
- Un solo nodo y una sola ejecución.
