# Metadata Store

Base de datos / almacenamiento de metadatos: qué broker es líder de cada partición, qué offsets tiene confirmado cada grupo de consumidores, y la membresía de cada grupo.

Puede ser una base de datos ligera embebida (SQLite) por nodo, o un almacén clave-valor simple, dado que el volumen de metadatos es bajo comparado con el de los eventos.
