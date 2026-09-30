# ray-prototype

Prototipo de la **Fase 1** (Python + Ray Core). Ray hace de andamiaje de middleware para entender el problema antes del diseño nativo.

- Un actor por partición con un log ordenado append-only y un registro de topics.
- Productor, consumidor y commit de offsets, ejecutándose sobre varios workers o procesos reales.
- Aquí también va el experimento inicial, que revela qué hace el runtime de Ray (crash de un worker, envíos concurrentes, etc.).

**Frontera de retirada:** a partir del Hito 2, Ray está **prohibido como dependencia runtime** de la implementación final. Esta carpeta solo se conserva como referencia para la comparación Ray/nativo de la Fase 3. Ningún código en Go puede depender de ella.
