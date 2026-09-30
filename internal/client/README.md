# internal/client

Biblioteca cliente en Go que usan los binarios `cmd/producer` y `cmd/consumer`.

- **Productor:** descubre el líder de cada partición a partir de una lista estática de brokers semilla, publica en lotes, y refresca los metadatos y reintenta ante "not leader" (escenario F10). Envía su ID de productor y un número de secuencia por registro para la deduplicación.
- **Consumidor:** hace fetch periódico desde el último offset, se une a un grupo, confirma offsets según la semántica de entrega elegida y maneja la revocación de particiones durante un rebalanceo.
