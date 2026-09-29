# Broker

Núcleo del servidor. Proceso que recibe mensajes de productores, los escribe en el log de la partición correspondiente y los sirve a los consumidores que los solicitan.

Expone las operaciones de red `produce(topic, partición, mensaje)` y `fetch(topic, partición, offset)` vía TCP/gRPC. Cada instancia corre en un proceso/contenedor distinto para simular nodos reales.
