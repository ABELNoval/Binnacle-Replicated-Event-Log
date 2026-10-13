# Experimento 2: se cae el dueño de las particiones (registro de topics)

Issue #29 · Fase 1 · prototipo `ray-prototype/`

## 1. Escenario

F1, *single process crash*, aplicado al proceso que **creó** las particiones: el registro de topics.

## 2. Workload

Un topic de 3 particiones con 5 registros en cada una. Después del fallo: lecturas de las tres particiones con los handles obtenidos antes del crash, y un cliente nuevo (`Producer`) que busca el topic desde cero.

## 3. Afirmación bajo prueba

Si muere el registro de topics, mueren también las particiones que creó, porque es su dueño (hipótesis 2 de `docs/architecture.md`).

## 4. Predicción

Escrita por Abel el **2026-10-13, antes de ejecutar**. Transcripción literal:

> 2.1 siguen vivas, dado que al ser detached aunque mates el proceso del registro este sigue eixistiendo.
> 2.2 diria que el Producer("exp2") funciona normal por dos motivos, al seguir existiendo el topic en si sigue pudiendo buscarlo, adeams inclsuo aunque no existiera seguiria funcionando normal proque lo crearia al ser un get_or_create.
> 2.3 si, si sigue existiendo. Sigue existiendo proque el cliente conoce o utuliza dicho topic desde una peticion de get_or_create tambien.

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

*Pendiente: se rellena después de ejecutar.*

## 8. Resultado observado

*Pendiente.*

## 9. Conclusión

*Pendiente.*

## 10. Incertidumbre restante

*Pendiente.*
