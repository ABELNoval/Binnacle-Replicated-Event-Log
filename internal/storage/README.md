# internal/storage

Log persistente en disco (WAL) de cada partición: archivos o segmentos append-only, con el offset asociado a cada registro y un CRC para detectar corrupción.

- Expone la interfaz `Log` que usa el broker (append, read, último offset, truncate).
- Política de `fsync` documentada en el código y en el registro de decisiones: qué se pierde ante un corte de energía.
- Recuperación al arrancar: reconstruye el índice de offsets y trunca la cola corrupta. Nunca acepta en silencio un registro corrupto.
- `Truncate` permite la reconciliación de réplicas con un log divergente.
