# internal/metadata

Almacén de metadatos: líder y epoch de cada partición, offsets confirmados por grupo de consumidores y membresía de los grupos.

Puede ser SQLite embebido o un almacén clave-valor simple, porque el volumen es bajo. Se permiten librerías de persistencia local, pero **no** un servicio distribuido externo que sustituya la coordinación. Dónde vive cada metadato y cómo se replica se decide en el registro de decisiones.
