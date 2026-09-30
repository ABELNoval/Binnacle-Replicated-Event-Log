# docs

Documentación del proyecto. Estos son los documentos que exige la metodología, en el orden en que se producen:

**Fase 1 (Hito 1)**
- `decisions.md`: decisiones del contrato gRPC (Fase 0).
- `contract.md`: contrato observable de comportamiento/API, sin detalles de Ray. Se congela en la puerta de salida.
- Diagrama de arquitectura y mapa de dependencias del middleware.
- `phase1-exit-gate.md`: respuestas escritas a las 5 preguntas de la puerta de salida.

**Fase 2 (Hitos 2 y 3)**
- Formato del log en disco.
- Registro de decisiones completo (requisito | Fase 1 Ray | alternativas nativas | diseño elegido | razón), con todos los temas de la metodología y la justificación de los mecanismos omitidos.
- Modelo de confianza/seguridad y postura ante particiones de red.

**Fase 3 (Hito 4)**
- `guarantees.md`: qué garantiza el sistema, dónde se cumple en el código y qué experimento lo respalda.
- `ray-vs-native.md`: comparación medida entre el prototipo y el sistema nativo.
- Informe final con la comparación con Kafka/Pulsar (la compatibilidad solo se afirma para el subconjunto implementado y probado).
- Declaración `ai_use` veraz y coherente con la evidencia del repositorio.
