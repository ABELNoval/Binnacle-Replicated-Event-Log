# proto

Contrato de red (gRPC + Protocol Buffers) entre los clientes y el broker: `broker.proto`.

- **Estado: provisional.** Según la metodología, el contrato observable se congela en la puerta de salida de la Fase 1. A partir de ahí, cualquier cambio debe justificarse explícitamente en el registro de decisiones.
- Los cambios deben ser aditivos (nuevos RPCs o campos con números nuevos), nunca renumerar ni reutilizar campos.
- El código Go generado se crea en el Hito 2, junto con `go.mod`, en el paquete acordado en `docs/decisions.md`.
