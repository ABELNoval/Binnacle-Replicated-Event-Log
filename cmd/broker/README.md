# cmd/broker

Punto de entrada del binario del broker: lee la configuración (ID del broker, lista de semillas, directorio de datos, timeouts, política de ack) desde flags o variables de entorno y arranca el servidor de `internal/broker`. Aquí no va lógica de negocio.
