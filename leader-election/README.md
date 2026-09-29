# Leader Election

Elección de líder y coordinación. Cuando el líder de una partición falla, otro broker (réplica) debe asumir el liderazgo sin perder datos confirmados.

Se implementa con un algoritmo de consenso simplificado (o Raft) apoyado en detección de fallos por heartbeats/timeouts.
