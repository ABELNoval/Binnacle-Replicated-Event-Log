# internal/metrics

Observabilidad del clúster (objetivo 12 del tema; es obligatorio, no opcional).

- Endpoint de métricas en JSON: lag por grupo y partición (high watermark menos offset confirmado), estado de las réplicas, líder actual por partición y brokers vivos/caídos.
- Convenciones de logs estructurados con IDs estables (broker, topic, partición, epoch, productor, generation del grupo), que sirven como evidencia en los experimentos.

El panel visual opcional que consume este endpoint vive en `web/`.
