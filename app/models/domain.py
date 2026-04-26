from dataclasses import asdict, dataclass
from typing import Optional


@dataclass(slots=True)
class SocioRecord:
    id: Optional[int]
    numero: int
    nombre: str
    dni: Optional[str]
    foto: Optional[str]
    meses: Optional[int]
    plazo_balance: Optional[int]
    cuotas: Optional[int]
    fecha_prestamo: Optional[str]
    saldo_base: float
    saldo_actual: float
    mes_2026: Optional[str]
    reunion: Optional[str]
    permisos: Optional[str]

    def to_dict(self):
        return asdict(self)


@dataclass(slots=True)
class ReunionRecord:
    periodo: str
    socio_numero: Optional[int]
    socio_nombre: Optional[str]
    estado: str
    fecha_programada: Optional[str]
    fecha_realizada: Optional[str]
    tipo_via: Optional[str]
    direccion_reunion: Optional[str]
    lugar_reunion: str
    observacion: Optional[str]
    actualizado_por: Optional[str]
    creado_en: Optional[str]
    actualizado_en: Optional[str]

    def to_dict(self):
        return asdict(self)


@dataclass(slots=True)
class PermisoRecord:
    id: int
    periodo: str
    socio_numero: int
    socio_nombre: str
    fecha_permiso: Optional[str]
    motivo: Optional[str]
    documento: Optional[str]
    observacion: Optional[str]
    registrado_por: Optional[str]
    creado_en: Optional[str]

    def to_dict(self):
        return asdict(self)


@dataclass(slots=True)
class AttendanceRecord:
    id: int
    socio_numero: int
    socio_nombre: str
    fecha: str
    estado: str
    observacion: Optional[str]

    def to_dict(self):
        return asdict(self)


@dataclass(slots=True)
class CajaItemRecord:
    socio_numero: int
    socio_nombre: str
    cuotas: Optional[int]
    fecha_prestamo: Optional[str]
    cuota_plazo: Optional[int]
    cuota_fecha: Optional[str]
    cuota_prestamo: float
    cuota_interes: float
    cuota_capital: float
    aporte_mensual: float
    total_mes: float
    saldo_actual: float
    fuente_saldo: Optional[str]

    def to_dict(self):
        return asdict(self)
