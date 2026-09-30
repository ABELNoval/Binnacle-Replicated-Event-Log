# internal/replication

Replicación del log de cada partición desde el líder hacia sus réplicas.

- Mantiene el conjunto de réplicas sincronizadas (in-sync) y el high watermark (último offset confirmado). Los consumidores solo leen hasta el high watermark.
- Implementa la política de ack (solo líder o líder + mínimo de réplicas sincronizadas), justificada en el registro de decisiones.
- Recuperación y reconciliación: un broker que vuelve trunca su sufijo divergente hasta el punto común con el líder actual y se pone al día antes de reincorporarse (escenario F7).
