# OS v2.0 — Poker Operating System
## Streamlit App

### Despliegue en Streamlit Cloud (recomendado)

1. Crea una cuenta gratuita en [streamlit.io](https://streamlit.io)
2. Crea un repositorio en GitHub (puede ser privado)
3. Sube estos 3 archivos:
   - `app.py`
   - `os_library.py`
   - `requirements.txt`
4. En Streamlit Cloud: "New app" → selecciona tu repo → `app.py`
5. Deploy. En ~2 minutos tienes tu URL fija.

### Uso local (alternativa)

```bash
pip install streamlit pandas numpy plotly scipy
streamlit run app.py
```

### Flujo de sesión

1. Abre la URL (móvil o escritorio)
2. En el panel izquierdo:
   - Escribe tu nick de PokerStars
   - Ajusta los sliders R/A/V post-sesión
   - Sube tu .txt exportado de PokerStars
3. Pulsa **▶ Ejecutar análisis**
4. Dashboard completo en ~10 segundos

### Qué incluye

- ✅ Parser HH real v1.99
- ✅ Métricas core (BB/100, EV/h, sesión actual)
- ✅ ROI ranking top leaks + oportunidades
- ✅ Drill activo con instrucción concreta
- ✅ M5 Pool fingerprint
- ✅ Comparativa hero vs referencia NL2 por posición
- ✅ Gráfico sesiones + tabla detalle
- ✅ Red line / Blue line
- ✅ KPIs con semáforos
- ✅ Detección tilt
- ✅ Progreso hacia M2
