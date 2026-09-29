# Consumer Groups

Cliente consumidor y grupos de consumidores. Procesos que leen eventos desde una partición, llevando su propio offset, y que pueden coordinarse en grupo para repartirse particiones.

Cada consumidor hace `fetch` periódico desde su último offset conocido; el grupo cuenta con un mecanismo de coordinación para asignar particiones y detectar caídas de miembros (rebalanceo).
