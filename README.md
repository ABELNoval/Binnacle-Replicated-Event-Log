# Binnacle · replicated-event-log

**Registro de eventos replicado y grupos de consumidores**

> *Binnacle* — del inglés, "bitácora": el soporte de la brújula junto al que se llevaba el registro de navegación.

Sistema de mensajería basado en un registro de eventos distribuido, particionado y replicado, con elección automática de líder y grupos de consumidores, inspirado en el modelo de Apache Kafka.

---

## Descripción

Binnacle es un sistema de mensajería construido desde cero sobre la idea de un **registro distribuido** (*distributed log*). Los componentes de un sistema no se comunican directamente entre sí, como en una llamada RPC clásica. Lo hacen a través de un registro persistente, ordenado y de solo escritura al final (*append-only*):

- Los **productores** escriben eventos en el registro.
- Los **consumidores** leen esos eventos a su propio ritmo.
- Productores y consumidores no necesitan estar activos al mismo tiempo, porque el registro los desacopla temporalmente.

## El problema

Escribir y leer una lista de eventos en una sola máquina es trivial. La complejidad aparece cuando el registro debe cumplir, a la vez, estas condiciones:

- **Escalar horizontalmente.** El registro se divide en particiones repartidas entre varios nodos (*brokers*). El orden se garantiza solo dentro de cada partición; no existe un orden global entre particiones distintas.
- **Sobrevivir a la caída de un nodo** sin perder datos ya confirmados. Para ello cada partición se replica en varios brokers y se define con precisión cuándo un mensaje se considera confirmado.
- **Elegir un nuevo líder de partición** automáticamente cuando el líder actual falla, sin perder mensajes ni duplicarlos de forma inconsistente.
- **Permitir que varios consumidores trabajen en grupo** sobre las mismas particiones y se repartan el trabajo. Si un miembro del grupo cae, otro asume sus particiones sin reprocesar todo desde el inicio.
- **Recordar la posición de cada consumidor** (*offset*), de modo que al reiniciarse continúe exactamente donde se quedó, sin repetir mensajes de más ni saltarse ninguno.
- **Manejar los reintentos de forma explícita.** Si un consumidor procesa un mensaje pero falla antes de confirmarlo, el sistema declara y justifica si ese mensaje se reprocesa (*at-least-once*) o se pierde (*at-most-once*).

## Enfoque

El objetivo de Binnacle no es solo implementar una cola de mensajes. Es diseñar la **infraestructura de coordinación** que mantiene esa cola correcta, ordenada y disponible aunque las máquinas que la sostienen fallen, se reinicien o se incorporen nodos nuevos.

## Conceptos principales

| Concepto | Descripción |
|---|---|
| **Topic** | Categoría lógica a la que se publican los eventos. |
| **Partición** | Subdivisión de un topic; unidad de orden, replicación y paralelismo. |
| **Broker** | Nodo del clúster que almacena particiones y atiende a productores y consumidores. |
| **Líder / Réplica** | Cada partición tiene un broker líder que recibe las escrituras y réplicas que mantienen copias. |
| **Offset** | Posición de un evento dentro de una partición; permite a cada consumidor retomar su lectura. |
| **Grupo de consumidores** | Conjunto de consumidores que se reparten las particiones de un topic. |
| **Rebalanceo** | Reasignación de particiones cuando un consumidor entra o sale del grupo. |

## Estado del proyecto

En desarrollo.

## Contexto académico

Proyecto Final — Sistemas Distribuidos 2026.
Tema P13: *Registro de eventos replicado y grupos de consumidores*.
