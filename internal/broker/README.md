# internal/broker

Núcleo del servidor: implementación del servicio gRPC definido en `proto/broker.proto` (`CreateTopic`, `Produce`, `Fetch`) y gestor de topics/particiones.

- Resuelve la partición destino de cada mensaje según la regla del contrato (partición explícita, hash de la key o round-robin).
- Solo el líder de una partición acepta escrituras; los demás responden "not leader".
- Nunca confirma un `Produce` antes de que se cumpla la durabilidad declarada (política de fsync + política de ack).
- Aplica los límites de backpressure (tamaño de lote, peticiones en vuelo).
- Logs estructurados con IDs estables (broker, topic, partición, epoch).

El binario que lo arranca vive en `cmd/broker`.
