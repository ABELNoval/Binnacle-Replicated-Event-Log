# Experimento 3: reinicio de consumidor tras caerse el almacén de offsets

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

Reinicio de consumidor (escenario específico de *event log* en la metodología del profesor), combinado con F1: muere el proceso que guarda los offsets confirmados.

## 2. Workload

Un topic de 1 partición con 10 registros. El consumidor A del grupo `g` procesa 6 y los confirma. Después del fallo, A intenta confirmar otro offset y se crea un consumidor B del mismo grupo que procesa todo lo que reciba.

## 3. Afirmación bajo prueba

Si muere el almacén de offsets, un consumidor nuevo vuelve a empezar desde 0 y reprocesa todo (hipótesis 4 de `docs/architecture.md`).

## 4. Predicción

Escrita por Abel el **2026-10-13, antes de ejecutar**. Transcripción literal:

> 3.1 empieza desde cero, porque no logra resolver el offset que llevaba el grupo g.
> 3.2 se procesan 6 registros dos veces dado que son los 2 registros que ya estaban en el grupo g. Se sigue cumpliendo la G9 porque se crean duplicados mas no se pierden registros como tal y la G7 si se incumple dado que B no comienza de su post-it, su post-it se perdio.
> 3.3 funciona dado que esto genra el nuevo Offset, pero obtiene un Offset que empieza con el offset en 0, no con los valores de antes

**Aclaración de Abel (2026-10-13, antes de ejecutar):** en la 3.2, el número de registros procesados dos veces es **6**.

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

*Pendiente: se rellena después de ejecutar.*

## 8. Resultado observado

*Pendiente.*

## 9. Conclusión

*Pendiente.*

## 10. Incertidumbre restante

*Pendiente.*
