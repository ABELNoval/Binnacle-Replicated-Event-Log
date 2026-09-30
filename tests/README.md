# tests

Pruebas de extremo a extremo y la campaña de fallos. Los tests unitarios de Go viven junto al código (`*_test.go`) y se ejecutan con `go test -race ./...`.

- **Pruebas públicas deterministas:** arrancan el clúster, ejecutan un workload y verifican el comportamiento esperado.
- **Harness de inyección de fallos:** detener/matar contenedores, retraso y pérdida de paquetes (`tc netem`), particiones de red (`docker network disconnect`) y recolección de evidencia.
- **Campaña mínima común:** caso normal, crash, crash/restart, degradación, partición + heal, duplicado/respuesta perdida, carga concurrente y sobrecarga.
- **Campaña específica del event log:** fallo del líder (F15), reinicio de consumidor y procesamiento duplicado.

Los registros de cada experimento (10 campos) se guardan en `experiments/`.
