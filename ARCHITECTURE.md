# Arquitectura JAWILVIO_V10

## Estado actual

La aplicación sigue siendo un sistema Flask monolítico en transición, pero ahora ya tiene una base modular para seguir creciendo sin concentrar todo en `app/routes.py` y `app/db.py`.

## Estructura nueva

### `app/core`
- `database.py`
  - Centraliza la configuración y apertura de conexiones.
  - Hoy usa SQLite.
  - Ya prepara el proyecto para un futuro `DB_ENGINE=postgresql`.

### `app/models`
- `domain.py`
  - Modelos de dominio en dataclasses:
    - `SocioRecord`
    - `ReunionRecord`
    - `PermisoRecord`
    - `AttendanceRecord`
    - `CajaItemRecord`

### `app/repositories`
- `association_repository.py`
  - Encapsula consultas SQL para:
    - socios
    - reuniones
    - permisos
    - asistencia
    - caja

### `app/services`
- `association_service.py`
  - Contiene reglas de negocio para:
    - altas, edición y bajas de socios
    - reuniones y permisos
    - asistencia
    - armado del payload de caja

### `app/utils`
- `validation.py`
  - Conversión segura de enteros y montos.
- `uploads.py`
  - Guardado y eliminación de:
    - fotos de socios
    - documentos de permisos
    - branding

### `app/route_modules`
- `socios_routes.py`
- `reuniones_routes.py`
- `asistencia_routes.py`
- `dashboard_routes.py`
- `monthly_routes.py`
- `report_routes.py`
- `admin_routes.py`
- `api_routes.py`

Estos módulos ya registran rutas reales sobre el blueprint principal `main`.

## Dominios que ya salieron del monolito

- socios
- reuniones y permisos
- asistencia
- dashboard
- cierre mensual
- nuevos préstamos
- reportes y saldo actual
- multas automáticas
- usuarios y configuración
- API REST de socios, reuniones, permisos y caja

## Capa legacy

Todavía permanece sobre todo en:
- `app/routes.py`
- `app/db.py`

pero ahora con menos responsabilidad directa, más enfocada en:
- helpers compartidos
- lógica histórica grande de préstamos
- importación del Excel
- flujos que aún no se han desacoplado del todo

## Estrategia de migración recomendada

### Fase 1
Ya realizada:
- mover infraestructura base
- crear repositorios y servicios
- separar módulos operativos principales

### Fase 2
Siguiente tramo recomendado:
- extraer `prestamos`
- extraer `aportaciones mensuales`
- extraer imprimibles grandes
- seguir reduciendo dependencias cruzadas desde `app/routes.py`

### Fase 3
Preparación para PostgreSQL:
- aislar SQL específico por repositorio
- unificar transacciones
- mover configuración de conexión a variables de entorno
- introducir migraciones formales si el proyecto sigue creciendo

## Objetivo

Que la app pueda evolucionar desde:
- un monolito funcional

hacia:
- una arquitectura por capas:
  - rutas
  - servicios
  - repositorios
  - modelos
  - utilidades

sin perder compatibilidad con SQLite ni romper las pantallas existentes.
