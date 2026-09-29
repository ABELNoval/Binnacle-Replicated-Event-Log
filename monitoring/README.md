# Monitoring

Panel de monitoreo (opcional pero recomendable). Interfaz visual simple para observar el estado del clúster: líder actual por partición, lag de cada consumidor, brokers vivos/caídos.

Backend expone un endpoint de métricas en JSON; un frontend simple lo consume y grafica. No es el foco del proyecto, pero facilita la demostración.
