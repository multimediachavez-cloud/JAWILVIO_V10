# JAWILVIO_V10

Plataforma Flask para la gestion integral de una asociacion financiera:

- socios
- prestamos
- aportaciones mensuales
- cierre mensual
- nuevos prestamos
- reuniones y permisos
- asistencia y multas automaticas
- reportes e imprimibles
- auditoria y respaldo

## Stack

- Python + Flask
- SQLite
- HTML + CSS + Jinja
- pandas + openpyxl para importacion del Excel

## Ejecucion local

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run.py
```

La app quedara disponible en:

- [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Usuarios iniciales

En desarrollo local el sistema puede sembrar usuarios base:

- `admin`
- `tesorero`
- `secretario`
- `consulta`

En produccion ya no conviene publicar contrasenas por defecto. El despliegue en Render queda preparado para crear solo el usuario `admin` usando la variable segura `ADMIN_BOOTSTRAP_PASSWORD`.

## Publicacion web recomendada

Este proyecto no es un sitio estatico tipo GitHub Pages. Es una aplicacion Flask dinamica, asi que debe desplegarse en un servicio web de Python como Render.

Ya queda listo para publicar con:

- [render.yaml](./render.yaml)
- [wsgi.py](./wsgi.py)
- `gunicorn` en [requirements.txt](./requirements.txt)

### Opcion recomendada: Render Blueprint

1. Sube el repo a GitHub.
2. En Render elige **New + > Blueprint**.
3. Selecciona este repositorio.
4. Render leera automaticamente [render.yaml](./render.yaml).
5. Elige un plan que soporte disco persistente.
6. Espera el primer deploy.

La app quedara publicada con una URL tipo:

- `https://jawilvio-v10.onrender.com`

## Persistencia en produccion

El despliegue ya contempla persistencia para:

- base de datos SQLite
- archivo Excel activo
- fotos de socios
- documentos de permisos
- logo institucional

Todo eso se guarda en el disco montado de Render:

- `/var/data`

## Variables importantes de entorno

Estas ya quedan definidas o preparadas en `render.yaml`:

- `SECRET_KEY`
- `ADMIN_BOOTSTRAP_PASSWORD`
- `SEED_DEFAULT_USERS`
- `JAWILVIO_INSTANCE_PATH`
- `JAWILVIO_UPLOADS_PATH`
- `DATABASE_PATH`
- `EXCEL_PATH`

## Health check

La aplicacion expone:

- `/health`

para verificaciones del despliegue.
