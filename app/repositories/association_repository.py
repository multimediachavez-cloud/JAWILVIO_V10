from app.models.domain import AttendanceRecord, CajaItemRecord, PermisoRecord, ReunionRecord, SocioRecord


def list_socios(conn, periodo, query=''):
    params = [periodo]
    where_clause = ""
    if query:
        where_clause = "WHERE s.nombre LIKE ? OR CAST(s.numero AS TEXT) LIKE ? OR COALESCE(s.dni, '') LIKE ?"
        params.extend([f'%{query}%', f'%{query}%', f'%{query}%'])
    rows = conn.execute(
        f"""
        SELECT s.*,
               COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
               COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
        FROM socios s
        LEFT JOIN cuotas c ON c.socio_numero = s.numero
        LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
        {where_clause}
        GROUP BY s.id, s.numero, s.nombre, s.dni, s.foto, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
        ORDER BY s.numero
        """,
        params,
    ).fetchall()
    return [_row_to_socio(row) for row in rows]


def get_socio(conn, numero, periodo):
    row = conn.execute(
        """
        SELECT s.*,
               COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
               COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
        FROM socios s
        LEFT JOIN cuotas c ON c.socio_numero = s.numero
        LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
        WHERE s.numero=?
        GROUP BY s.id, s.numero, s.nombre, s.dni, s.foto, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
        """,
        (periodo, numero),
    ).fetchone()
    return _row_to_socio(row) if row else None


def get_socio_raw(conn, numero):
    return conn.execute("SELECT * FROM socios WHERE numero=?", (numero,)).fetchone()


def list_socios_basic(conn):
    return conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()


def upsert_socio(conn, numero, nombre, dni, foto, meses, fecha_prestamo, saldo, mes_2026):
    conn.execute(
        """
        INSERT OR REPLACE INTO socios(
            id, numero, nombre, dni, foto, meses, plazo_balance, fecha_prestamo, saldo, mes_2026, reunion, permisos
        )
        VALUES(
            COALESCE((SELECT id FROM socios WHERE numero=?), NULL), ?, ?, ?, ?, ?, 
            COALESCE((SELECT plazo_balance FROM socios WHERE numero=?), NULL),
            ?, ?, ?, 
            COALESCE((SELECT reunion FROM socios WHERE numero=?), NULL),
            COALESCE((SELECT permisos FROM socios WHERE numero=?), NULL)
        )
        """,
        (
            numero,
            numero,
            nombre,
            dni,
            foto,
            meses,
            numero,
            fecha_prestamo,
            saldo,
            mes_2026,
            numero,
            numero,
        ),
    )


def delete_socio_related(conn, numero):
    socio = conn.execute("SELECT numero, nombre, foto FROM socios WHERE numero=?", (numero,)).fetchone()
    if not socio:
        return None
    historial_ids = [row['id'] for row in conn.execute("SELECT id FROM historial_prestamos_socios WHERE socio_numero=?", (numero,)).fetchall()]
    prestamo_excel_ids = [row['id'] for row in conn.execute("SELECT id FROM prestamos_excel_historial WHERE socio_numero=?", (numero,)).fetchall()]
    for historial_id in historial_ids:
        conn.execute("DELETE FROM historial_prestamos_socios_cuotas WHERE historial_id=?", (historial_id,))
    for prestamo_excel_id in prestamo_excel_ids:
        conn.execute("DELETE FROM prestamos_excel_historial_cuotas WHERE prestamo_excel_id=?", (prestamo_excel_id,))
    conn.execute("DELETE FROM cuotas WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM asistencia WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM saldo_historico_mensual WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM obligaciones_mensuales WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM aportaciones_mensuales WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM obligaciones_mensuales_override WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM prioridad_colocacion_manual WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM prestamos_nuevos WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM historial_prestamos_socios WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM prestamos_excel_historial WHERE socio_numero=?", (numero,))
    conn.execute("DELETE FROM socios WHERE numero=?", (numero,))
    return socio


def get_reunion(conn, periodo):
    row = conn.execute("SELECT * FROM reuniones_mensuales WHERE periodo=?", (periodo,)).fetchone()
    return _row_to_reunion(row) if row else None


def list_reuniones(conn):
    rows = conn.execute("SELECT * FROM reuniones_mensuales ORDER BY periodo DESC").fetchall()
    return [_row_to_reunion(row) for row in rows]


def save_reunion(conn, periodo, socio_numero, socio_nombre, estado, fecha_programada, fecha_realizada, tipo_via, direccion_reunion, observacion, actualizado_por):
    conn.execute(
        """
        INSERT OR REPLACE INTO reuniones_mensuales(
            periodo, socio_numero, socio_nombre, estado, fecha_programada, fecha_realizada, tipo_via, direccion_reunion, observacion, actualizado_por, actualizado_en
        ) VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        """,
        (
            periodo,
            socio_numero,
            socio_nombre,
            estado,
            fecha_programada,
            fecha_realizada,
            tipo_via,
            direccion_reunion,
            observacion,
            actualizado_por,
        ),
    )


def delete_reunion(conn, periodo):
    conn.execute("DELETE FROM reuniones_mensuales WHERE periodo=?", (periodo,))


def list_permisos(conn, periodo=None, socio_numero=None):
    params = []
    where = []
    if periodo:
        where.append("periodo=?")
        params.append(periodo)
    if socio_numero is not None:
        where.append("socio_numero=?")
        params.append(socio_numero)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ''
    rows = conn.execute(
        f"""
        SELECT *
        FROM permisos_mensuales
        {where_sql}
        ORDER BY periodo DESC, fecha_permiso DESC, id DESC
        """,
        params,
    ).fetchall()
    return [_row_to_permiso(row) for row in rows]


def get_permiso(conn, permiso_id):
    row = conn.execute("SELECT * FROM permisos_mensuales WHERE id=?", (permiso_id,)).fetchone()
    return _row_to_permiso(row) if row else None


def insert_permiso(conn, periodo, socio_numero, socio_nombre, fecha_permiso, motivo, documento, observacion, registrado_por):
    cur = conn.execute(
        """
        INSERT INTO permisos_mensuales(
            periodo, socio_numero, socio_nombre, fecha_permiso, motivo, documento, observacion, registrado_por
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (periodo, socio_numero, socio_nombre, fecha_permiso, motivo, documento, observacion, registrado_por),
    )
    return get_permiso(conn, cur.lastrowid)


def update_permiso(conn, permiso_id, fecha_permiso, motivo, documento, observacion):
    conn.execute(
        """
        UPDATE permisos_mensuales
        SET fecha_permiso=?, motivo=?, documento=?, observacion=?
        WHERE id=?
        """,
        (fecha_permiso, motivo, documento, observacion, permiso_id),
    )


def delete_permiso(conn, permiso_id):
    conn.execute("DELETE FROM permisos_mensuales WHERE id=?", (permiso_id,))


def count_permisos(conn, periodo):
    return int(conn.execute("SELECT COUNT(*) FROM permisos_mensuales WHERE periodo=?", (periodo,)).fetchone()[0] or 0)


def list_periodos(conn):
    return [row['periodo'] for row in conn.execute("SELECT periodo FROM periodos ORDER BY periodo DESC").fetchall()]


def get_periodo_row(conn, periodo):
    return conn.execute("SELECT * FROM periodos WHERE periodo=?", (periodo,)).fetchone()


def get_cierre_row(conn, periodo):
    return conn.execute("SELECT * FROM cierre_mensual WHERE periodo=?", (periodo,)).fetchone()


def list_obligaciones(conn, periodo):
    rows = conn.execute(
        """
        SELECT socio_numero, socio_nombre, cuotas, fecha_prestamo, cuota_plazo, cuota_fecha,
               cuota_prestamo, cuota_interes, cuota_capital, aporte_mensual, total_mes, saldo_actual, fuente_saldo
        FROM obligaciones_mensuales
        WHERE periodo=?
        ORDER BY socio_numero
        """,
        (periodo,),
    ).fetchall()
    return [_row_to_caja_item(row) for row in rows]


def list_recent_attendance(conn, limit=50):
    rows = conn.execute("SELECT * FROM asistencia ORDER BY fecha DESC, id DESC LIMIT ?", (limit,)).fetchall()
    return [_row_to_attendance(row) for row in rows]


def get_attendance_record(conn, registro_id):
    row = conn.execute("SELECT * FROM asistencia WHERE id=?", (registro_id,)).fetchone()
    return _row_to_attendance(row) if row else None


def insert_attendance(conn, socio_numero, socio_nombre, fecha, estado, observacion):
    cur = conn.execute(
        "INSERT INTO asistencia(socio_numero, socio_nombre, fecha, estado, observacion) VALUES(?,?,?,?,?)",
        (socio_numero, socio_nombre, fecha, estado, observacion),
    )
    return get_attendance_record(conn, cur.lastrowid)


def update_attendance(conn, registro_id, socio_numero, socio_nombre, fecha, estado, observacion):
    conn.execute(
        """
        UPDATE asistencia
        SET socio_numero=?, socio_nombre=?, fecha=?, estado=?, observacion=?
        WHERE id=?
        """,
        (socio_numero, socio_nombre, fecha, estado, observacion, registro_id),
    )
    return get_attendance_record(conn, registro_id)


def delete_attendance(conn, registro_id):
    conn.execute("DELETE FROM asistencia WHERE id=?", (registro_id,))


def _row_to_socio(row):
    if not row:
        return None
    return SocioRecord(
        id=row['id'],
        numero=row['numero'],
        nombre=row['nombre'],
        dni=row['dni'],
        foto=row['foto'],
        meses=row['meses'],
        plazo_balance=row['plazo_balance'],
        cuotas=row['plazo_real'] or row['meses'],
        fecha_prestamo=row['fecha_prestamo'],
        saldo_base=round(float(row['saldo'] or 0), 1),
        saldo_actual=round(float(row['saldo_periodo'] or row['saldo'] or 0), 1),
        mes_2026=row['mes_2026'],
        reunion=row['reunion'],
        permisos=row['permisos'],
    )


def _row_to_reunion(row):
    if not row:
        return None
    lugar = ' '.join([parte.strip() for parte in [(row['tipo_via'] or ''), (row['direccion_reunion'] or '')] if parte and parte.strip()]).strip() or '-'
    return ReunionRecord(
        periodo=row['periodo'],
        socio_numero=row['socio_numero'],
        socio_nombre=row['socio_nombre'],
        estado=row['estado'],
        fecha_programada=row['fecha_programada'],
        fecha_realizada=row['fecha_realizada'],
        tipo_via=row['tipo_via'],
        direccion_reunion=row['direccion_reunion'],
        lugar_reunion=lugar,
        observacion=row['observacion'],
        actualizado_por=row['actualizado_por'],
        creado_en=row['creado_en'],
        actualizado_en=row['actualizado_en'],
    )


def _row_to_permiso(row):
    if not row:
        return None
    return PermisoRecord(
        id=row['id'],
        periodo=row['periodo'],
        socio_numero=row['socio_numero'],
        socio_nombre=row['socio_nombre'],
        fecha_permiso=row['fecha_permiso'],
        motivo=row['motivo'],
        documento=row['documento'],
        observacion=row['observacion'],
        registrado_por=row['registrado_por'],
        creado_en=row['creado_en'],
    )


def _row_to_attendance(row):
    if not row:
        return None
    return AttendanceRecord(
        id=row['id'],
        socio_numero=row['socio_numero'],
        socio_nombre=row['socio_nombre'],
        fecha=row['fecha'],
        estado=row['estado'],
        observacion=row['observacion'],
    )


def _row_to_caja_item(row):
    return CajaItemRecord(
        socio_numero=row['socio_numero'],
        socio_nombre=row['socio_nombre'],
        cuotas=row['cuotas'],
        fecha_prestamo=row['fecha_prestamo'],
        cuota_plazo=row['cuota_plazo'],
        cuota_fecha=row['cuota_fecha'],
        cuota_prestamo=round(float(row['cuota_prestamo'] or 0), 1),
        cuota_interes=round(float(row['cuota_interes'] or 0), 1),
        cuota_capital=round(float(row['cuota_capital'] or 0), 1),
        aporte_mensual=round(float(row['aporte_mensual'] or 0), 1),
        total_mes=round(float(row['total_mes'] or 0), 1),
        saldo_actual=round(float(row['saldo_actual'] or 0), 1),
        fuente_saldo=row['fuente_saldo'],
    )
