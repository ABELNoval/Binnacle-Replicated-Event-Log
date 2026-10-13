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

Los scripts que ejecutan los escenarios viven en `tests/` (o en `ray-prototype/` para los de la Fase 1). Aquí van los registros y la evidencia.

## Registros

| Experimento | Escenario | Predicción | Resultado |
|---|---|---|---|
| [exp1-partition-crash.md](exp1-partition-crash.md) | F1: cae una partición mientras se produce | 6 de 6 | Confirma la pérdida del log y la ausencia de reinicio |
| [exp2-registry-crash.md](exp2-registry-crash.md) | F1 sobre el dueño: cae el registro de topics | 0 de 3 | Confirma el *fate-sharing*; la pérdida del topic es silenciosa |
| [exp3-offset-store-crash.md](exp3-offset-store-crash.md) | Reinicio de consumidor tras caer el almacén de offsets | 2 de 3 | Confirma el reinicio desde 0: 6 duplicados, G7 rota |

Evidencia en `evidence/`: un JSONL por experimento y la salida de consola (`console.log`). Script: `ray-prototype/fault_experiments.py`.
