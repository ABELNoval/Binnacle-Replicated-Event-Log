# internal/group

Coordinador de grupos de consumidores.

- Membresía: join, heartbeat y leave, con timeout de sesión.
- Asignación de particiones entre los miembros y rebalanceo automático cuando uno entra, sale o deja de responder.
- Los generation IDs rechazan commits y fetches de miembros obsoletos.
- Los offsets confirmados se guardan en `internal/metadata`. La semántica de entrega (at-least-once, etc.) se declara en el registro de decisiones y se demuestra con experimentos (F8, F9, F18).
