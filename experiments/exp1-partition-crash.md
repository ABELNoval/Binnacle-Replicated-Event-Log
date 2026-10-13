# Experimento 1 (F1): se cae una partición mientras se produce

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

F1, *single process crash*: muere de forma abrupta el proceso que aloja una partición mientras un productor está escribiendo.

## 2. Workload

Un productor envía registros sin key a un topic de 3 particiones, así que el round-robin los reparte por igual. Primero 30 registros (10 por partición) y, tras el fallo, 30 más con el mismo reparto. Cada `send` espera la confirmación antes del siguiente.

## 3. Afirmación bajo prueba

Si muere el proceso de una partición, se pierde su log y la partición queda inutilizable (hipótesis 1 de `docs/architecture.md`).

## 4. Predicción

Escrita por Abel el **2026-10-13, antes de ejecutar**. Transcripción literal:

> 1.1 la b, o sea da un error
> 1.2 El erro que se espera es uno de Ray, RayActorError
> 1.3 Los sends a la demas particiones funcionan correctamente.
> 1.4 Da error.
> 1.5 No la revivie, nosotros no menjamos el max_restarts > 0
> 1.6 la a, devulve el mismo actor muerto y usarlo falla

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

*Pendiente: se rellena después de ejecutar.*

## 8. Resultado observado

*Pendiente.*

## 9. Conclusión

*Pendiente.*

## 10. Incertidumbre restante

*Pendiente.*
