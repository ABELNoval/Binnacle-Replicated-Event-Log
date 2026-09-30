# internal/election

Detección de fallos y elección de líder por partición.

- Heartbeats y timeouts configurables; su valor se justifica en el registro de decisiones.
- Algoritmo elegido por el equipo (Raft, versión simplificada o coordinador propio), implementado aquí. **Prohibido** delegar el consenso o la elección de líder a ZooKeeper, etcd o similares.
- Los epochs/terms impiden que existan dos líderes a la vez. Solo una réplica sincronizada puede ser elegida, para no perder datos confirmados.
- Postura ante una partición de red: qué lado puede seguir aceptando escrituras.
