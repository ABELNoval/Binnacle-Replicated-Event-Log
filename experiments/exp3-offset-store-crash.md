# Experimento 3: reinicio de consumidor tras caerse el almacén de offsets

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

Reinicio de consumidor (escenario específico de *event log* en la metodología del profesor), combinado con F1: muere el proceso que guarda los offsets confirmados.

## 2. Workload

Un topic de 1 partición con 10 registros. El consumidor A del grupo `g` procesa 6 y los confirma. Después del fallo, A intenta confirmar otro offset y se crea un consumidor B del mismo grupo que procesa todo lo que reciba.

## 3. Afirmación bajo prueba

Si muere el almacén de offsets, un consumidor nuevo vuelve a empezar desde 0 y reprocesa todo (hipótesis 4 de `docs/architecture.md`).

## 4. Predicción

Registrada el **2026-10-13, antes de la ejecución**, en el commit `bf7cebe`. Lo que sigue es la redacción formal de esa predicción, sin cambios de contenido; el texto original y una aclaración de la 3.2 (6 registros duplicados), también previa a la ejecución, constan en dicho commit.

- **3.1** Opción (b): el consumidor B empieza desde el offset 0, porque no puede recuperar el offset confirmado del grupo `g`.
- **3.2** Se procesan dos veces 6 registros: los ya confirmados por el grupo `g`. G9 se sigue cumpliendo, porque se producen duplicados pero no se pierde ningún registro. G7 se incumple, porque B no reanuda desde el offset confirmado del grupo, que se ha perdido.
- **3.3** Opción (a): el commit funciona, porque genera un nuevo almacén de offsets; pero ese almacén empieza en el offset 0, no con los valores anteriores.

Preguntas a las que responde:

- 3.1 El consumidor B nuevo: (a) reanuda desde el offset 6; (b) empieza desde 0; (c) falla al crearse; (d) se cuelga.
- 3.2 Si empieza desde 0, ¿cuántos registros se procesan dos veces? ¿Se siguen cumpliendo G9 (at-least-once) y G7 (reanudación)?
- 3.3 El commit del consumidor A con el handle viejo: (a) funciona; (b) da error; (c) se cuelga.

## 5. Configuración

- Un nodo de Ray en Windows 11, Python 3.13.0, Ray 2.58.0 (`ray-prototype/requirements.txt`).
- Código del prototipo sin modificar. Los dos consumidores usan el almacén por defecto, `get_or_create_offset_store()`: actor con nombre, *detached*, `get_if_exists=True`. `run()` confirma cada registro después de procesarlo.
- Topic `exp3` con 1 partición; grupo `g`.
- Una operación que tarda más de 10 s cuenta como "colgada".
- Script: `ray-prototype/fault_experiments.py`.

## 6. Trigger del fallo

Después de que A procese y confirme 6 registros, se obtiene el PID del proceso del `OffsetStore` (ejecutando `os.getpid()` dentro del actor) y se mata con `taskkill /F /PID <pid>`.

## 7. Evidencia recogida

Ejecutado el 2026-10-13 a las 08:49 (hora local), después del commit `bf7cebe` que contiene la predicción.

- `experiments/evidence/exp3.jsonl`: 11 líneas.
- `experiments/evidence/console.log`: aviso del raylet sobre el worker 30280 (el `OffsetStore`), `SYSTEM_ERROR`.

## 8. Resultado observado

| Paso | Observado |
|---|---|
| Consumidor A, `run(max_messages=6)` | Procesa los offsets 0 a 5; offset confirmado `{0: 6}` |
| `taskkill` del `OffsetStore` (PID 30280) | Código 0; el proceso deja de existir |
| A confirma con el handle viejo | Falla al instante con `ActorDiedError` |
| Crear el consumidor B | Funciona (0,039 s). Posición inicial `{0: 0}`: `get_or_create_offset_store()` creó un almacén **nuevo y vacío** |
| B procesa | 10 registros: offsets 0 a 9 |
| Procesados dos veces | **6** (offsets 0 a 5) |
| Offset confirmado al final | `{0: 10}`, en el almacén nuevo |

**Comparación con la predicción: 2 de 3.**

| Pregunta | Predicción | Observado | |
|---|---|---|---|
| 3.1 | (b) empieza desde 0 | Posición inicial 0 | ✅ |
| 3.2 | 6 duplicados; G9 se cumple, G7 no | 6 duplicados (0 a 5); ninguno se pierde; no reanuda desde 6 | ✅ |
| 3.3 | (a) funciona, porque se crea un almacén nuevo | (b) `ActorDiedError` | ❌ |

**Por qué falló la 3.3:** A no vuelve a buscar el almacén por nombre; guarda el handle que obtuvo al crearse, y ese handle apunta al actor muerto. El almacén nuevo solo lo crea quien llama a `get_or_create_offset_store()` después de la caída, en este caso el consumidor B al construirse.

## 9. Conclusión

Se **confirma** la afirmación. Al morir el almacén de offsets:

- **G9 (at-least-once) se mantiene:** ningún registro se pierde; los 6 ya procesados se procesan otra vez.
- **G7 (reanudación) se rompe:** B no puede distinguir "este grupo nunca confirmó nada" de "se perdieron los offsets", así que empieza en 0. La cantidad de duplicados crece con todo lo que el grupo había confirmado.
- **Dos consumidores del mismo grupo terminan viendo almacenes distintos:** A falla con el viejo y B trabaja con uno nuevo. Nadie avisa de que el estado del grupo se reinició.

**Para la Fase 2:** los offsets confirmados deben ser durables y replicados (Tarea 12), y la respuesta para un grupo desconocido debe ser explícita, no un 0 silencioso.

## 10. Incertidumbre restante

- No se probó la caída del almacén **mientras** un consumidor confirma (¿qué commit queda?).
- No se probó qué pasa si el almacén se cae y un consumidor que sigue vivo continúa: falla en su próximo commit, pero no se observó qué hace después su bucle `run()`.
- Un solo nodo y una sola ejecución.
