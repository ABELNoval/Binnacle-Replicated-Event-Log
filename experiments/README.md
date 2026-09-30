# experiments

Registro experimental: la evidencia de lo que el sistema garantiza realmente. Hay un archivo por experimento significativo, con estos 10 campos:

1. Escenario (F0–F18 del vocabulario del curso)
2. Workload
3. Afirmación bajo prueba
4. Predicción (escrita **antes** de ejecutar)
5. Configuración
6. Trigger del fallo
7. Evidencia recogida (logs, métricas, estado persistente, trazas; las capturas solo sirven de apoyo)
8. Resultado observado
9. Conclusión
10. Incertidumbre restante

Los scripts que ejecutan los escenarios viven en `tests/`. Aquí van los registros y la evidencia.
