# Documentación de Contexto - Consums_v3

Esta carpeta contiene **documentación de contexto persistente** para facilitar el mantenimiento y desarrollo del proyecto.

## 📋 Archivos Disponibles

### [PROJECT.md](PROJECT.md)
**Estado actual del proyecto**
- Qué está en producción (v3.1)
- Últimos desarrollos completados
- Trabajo en curso y mejoras futuras
- Operaciones comunes (reprocesar día, validación manual, etc.)
- Métricas de producción
- Monitoreo y mantenimiento

### [RULES.md](RULES.md)
**Las 7 reglas de negocio críticas**
1. per10 Multiplier (18 señales)
2. Detección de Resets de Contador
3. Última Hora del Día (23:00)
4. Timestamps de Validación (00:00, no 23:59)
5. Encoding Windows Server (cp1252)
6. Permisos PostgreSQL (manejo de errores)
7. Idioma Català (output usuario)

Cada regla incluye:
- Qué hace y por qué existe
- Ubicación exacta del código (módulo, función)
- Código ejemplo
- Errores comunes a evitar
- Impacto si se rompe

### [ARCHITECTURE.md](ARCHITECTURE.md)
**Arquitectura completa del sistema**
- Diagrama de flujo general (ASCII art)
- Tabla de responsabilidades por módulo
- Flujo de datos detallado (transformaciones paso a paso)
- Estructura de archivos y directorios
- Integraciones externas (API SagedCAT, PostgreSQL, OAuth2)
- Flujo temporal (ejecución diaria típica)
- Puntos de decisión
- Escalabilidad y performance

### [DECISIONS.md](DECISIONS.md)
**Decisiones técnicas documentadas**
- 12 decisiones importantes con contexto completo
- Alternativas consideradas
- Razones de la decisión
- Implementación (código ejemplo)
- Consecuencias (ventajas y limitaciones)
- Estado (implementado/en progreso/rechazado)

Incluye decisiones sobre:
- Python 3.13, psycopg v3, OAuth2 email
- Vista PostgreSQL, CSV europeo, Submódulo Git
- Task Scheduler, DELETE+INSERT, Procesamiento 3 fases
- per10 en adquisición, PDF horizontal, Keep a Changelog

## 🎯 Cuándo Usar Esta Documentación

### Antes de Modificar Código
1. ✅ Lee **RULES.md** → Entiende qué reglas de negocio afectas
2. ✅ Revisa **ARCHITECTURE.md** → Entiende el flujo de datos
3. ✅ Consulta **PROJECT.md** → Conoce el estado actual
4. ✅ Revisa **DECISIONS.md** → Entiende por qué se hizo así

### Después de Hacer Cambios
1. ✅ Actualiza **CHANGELOG.md** (formato Keep a Changelog)
2. ✅ Si afecta reglas: actualiza **RULES.md**
3. ✅ Si afecta arquitectura: actualiza **ARCHITECTURE.md**
4. ✅ Si es decisión técnica: documenta en **DECISIONS.md**
5. ✅ Actualiza **PROJECT.md** con el nuevo estado

### Al Incorporarse al Proyecto
Lee en este orden:
1. **PROJECT.md** → Estado actual, qué hay en producción
2. **RULES.md** → Las reglas críticas que NO debes romper
3. **ARCHITECTURE.md** → Cómo funciona el sistema
4. **DECISIONS.md** → Por qué se tomaron ciertas decisiones técnicas

### Al Debuggear un Problema
1. **RULES.md** → Verifica si se está violando alguna regla
2. **ARCHITECTURE.md** → Identifica dónde está el problema en el flujo
3. **DECISIONS.md** → Entiende el contexto de la implementación

## 🔄 Mantenimiento de Esta Documentación

**Esta documentación debe mantenerse actualizada**. Es tan importante como el código.

### Responsabilidades
- **Desarrollador que hace el cambio**: Actualiza documentación afectada
- **Revisor del PR**: Verifica que documentación esté actualizada
- **Mantenedor del proyecto**: Revisa periódicamente consistencia

### Indicadores de Documentación Desactualizada
⚠️ Si encuentras alguno de estos, actualiza la documentación:
- Código no coincide con lo documentado en RULES.md
- Flujo de datos cambió pero ARCHITECTURE.md no se actualizó
- Nueva decisión técnica importante no documentada en DECISIONS.md
- Estado del proyecto cambió pero PROJECT.md no refleja la realidad

## 📚 Documentación Complementaria

Esta carpeta `context/` complementa la documentación existente:

- **README.md** (raíz) → Guía de usuario, instalación, uso
- **CHANGELOG.md** (raíz) → Historial de cambios
- **.github/copilot-instructions.md** → Instrucciones para GitHub Copilot
- **docs/** → Documentación operativa (email setup, automatización, etc.)

## ✅ Checklist al Hacer Cambios

```
[ ] Código modificado
[ ] Tests pasando (si aplica)
[ ] CHANGELOG.md actualizado
[ ] context/RULES.md actualizado (si afecta reglas)
[ ] context/ARCHITECTURE.md actualizado (si afecta flujo)
[ ] context/DECISIONS.md actualizado (si es decisión importante)
[ ] context/PROJECT.md actualizado (si cambia estado)
[ ] README.md actualizado (si afecta uso)
```

---

**Propósito de esta carpeta**: Mantener contexto técnico persistente que permita a cualquier desarrollador (o IA) entender rápidamente el sistema y trabajar con confianza, conociendo las reglas críticas, la arquitectura, y el razonamiento detrás de las decisiones técnicas.
