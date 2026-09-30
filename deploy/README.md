# deploy

Contenerización con Docker y Docker Compose: simula un clúster real de varios brokers en una sola máquina.

- Un contenedor por broker (y por productor/consumidor cuando haga falta), con una red interna compartida y un volumen de datos por broker.
- Todos los parámetros de despliegue son configurables (ID del broker, semillas, directorio de datos, timeouts, política de ack).
- Las caídas se simulan deteniendo contenedores individuales. Los retrasos, las pérdidas y las particiones de red se inyectan desde los scripts de `tests/`.
- La ejecución es 100 % local y reproducible.
