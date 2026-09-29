# Storage

Almacenamiento persistente del log en disco. Aquí viven realmente los mensajes de cada partición, de forma que sobrevivan a un reinicio del broker.

Se implementa como archivo(s)/segmentos por partición, con append-only y offset asociado a cada mensaje. Incluye la política de `fsync` a disco (durabilidad vs. rendimiento).
