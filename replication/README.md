# Replication

Replicación entre brokers. Cada partición tiene un broker líder y uno o más brokers réplica que reciben copia de los mensajes.

El líder recibe la escritura, la aplica localmente y la reenvía a las réplicas; el productor recibe el "ack" según la política definida (solo líder, o líder + mínimo de réplicas).
