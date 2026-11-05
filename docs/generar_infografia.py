"""Genera docs/infografia-jacobi.svg:  python docs/generar_infografia.py docs/infografia-jacobi.svg"""

import re
import sys
from html import escape

W = 1000
out: list[str] = []


def fmt(s: str) -> str:
    """Mini-markup: a_{ij} -> subíndice, x^{k} -> superíndice (con tspan + dy)."""
    s = escape(s, quote=False)
    res, pos = [], 0
    for mm in re.finditer(r"([_^])\{([^}]*)\}", s):
        res.append(s[pos : mm.start()])
        dy = 4 if mm.group(1) == "_" else -6
        res.append(f'<tspan dy="{dy}" font-size="72%">{mm.group(2)}</tspan><tspan dy="{-dy}">​</tspan>')
        pos = mm.end()
    res.append(s[pos:])
    return "".join(res)


def t(x, y, s, cls="b", anchor="start", extra=""):
    out.append(f'<text x="{x}" y="{y}" class="{cls}" text-anchor="{anchor}" {extra}>{fmt(s)}</text>')


def rect(x, y, w, h, cls, rx=0, extra=""):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" class="{cls}" {extra}/>')


def card(x, y, w, h, n, titulo):
    rect(x, y, w, h, "card", 14)
    out.append(f'<circle cx="{x + 30}" cy="{y + 32}" r="14" class="num"/>')
    t(x + 30, y + 37, str(n), "numt", "middle")
    t(x + 54, y + 38, titulo, "h2")


def bar_h(x0, y, w, h, title):
    """Barra horizontal: cuadrada en la base, 4px redondeada en el extremo."""
    r = min(4, w / 2)
    d = f"M{x0},{y} h{w - r} a{r},{r} 0 0 1 {r},{r} v{h - 2 * r} a{r},{r} 0 0 1 {-r},{r} h{-(w - r)} z"
    out.append(f'<path d="{d}" class="bar"><title>{escape(title)}</title></path>')


def bar_v(x, ybase, w, h, title):
    r = min(4, h / 2)
    d = f"M{x},{ybase} v{-(h - r)} a{r},{r} 0 0 1 {r},{-r} h{w - 2 * r} a{r},{r} 0 0 1 {r},{r} v{h - r} z"
    out.append(f'<path d="{d}" class="bar"><title>{escape(title)}</title></path>')


def arrow(x1, y1, x2, y2):
    out.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="arrow" marker-end="url(#punta)"/>')


# ---------------------------------------------------------------- encabezado
t(40, 62, "Jacobi por bloques distribuido: por qué funciona", "h1")
t(40, 92, "Placa de Laplace 40×40 (n = 1 600 incógnitas), 4 bloques de 400 filas, tolerancia ‖b − A·x‖/‖b‖ ≤ 10^{−8}", "sub")

# ---------------------------------------------------------------- 1. problema
Y = 120
card(30, Y, 460, 380, 1, "El problema: una placa, cuatro esclavos")
gx, gy, cs, k = 70, Y + 100, 26, 8
out.append(f'<line x1="{gx}" y1="{gy - 8}" x2="{gx + k * cs}" y2="{gy - 8}" class="hot"/>')
t(gx + k * cs / 2, gy - 16, "borde superior a 100°", "small", "middle")
for i in range(k):
    for j in range(k):
        rect(gx + j * cs + 1, gy + i * cs + 1, cs - 2, cs - 2, f"s{i // 2 + 1} cell", 3)
ci, cj = 3, 4
for di, dj in ((0, 0), (-1, 0), (1, 0), (0, -1), (0, 1)):
    rect(gx + (cj + dj) * cs + 1, gy + (ci + di) * cs + 1, cs - 2, cs - 2, "sten" if di or dj else "stenc", 3)
for s in range(4):
    yy = gy + s * 2 * cs + cs
    t(gx + k * cs + 14, yy + 5, f"esclavo {s + 1}", "lab")
    out.append(f'<rect x="{gx + k * cs + 84}" y="{yy - 6}" width="10" height="10" rx="2" class="s{s + 1}"/>')
t(60, Y + 340, "Cada celda: 4·x_{c} − x_{N} − x_{S} − x_{E} − x_{O} = b_{c}  (5 puntos)", "b")
t(60, Y + 362, "Cada esclavo recibe una franja de filas consecutivas de A.", "muted")

# ---------------------------------------------------------------- 2. matriz
card(510, Y, 460, 380, 2, "La matriz: A = M − N")
mx, my, bs = 560, Y + 80, 52
for i in range(4):
    for j in range(4):
        if i == j:
            rect(mx + j * bs + 2, my + i * bs + 2, bs - 4, bs - 4, f"s{i + 1}", 4)
            t(mx + j * bs + bs / 2, my + i * bs + bs / 2 + 5, f"A_{{{i + 1}{i + 1}}}", "onfill" if i < 2 else "onfilld", "middle")
        elif abs(i - j) == 1:
            rect(mx + j * bs + 2, my + i * bs + 2, bs - 4, bs - 4, "acople", 4)
        else:
            rect(mx + j * bs + 2, my + i * bs + 2, bs - 4, bs - 4, "vacio", 4)
lx = mx + 4 * bs + 24
[rect(lx + (q % 2) * 8, my + 6 + (q // 2) * 8, 6, 6, f"s{q + 1}", 1) for q in range(4)]
t(lx + 22, my + 18, "M = diag(A_{BB})", "b")
t(lx + 22, my + 38, "lo que resuelve", "muted")
t(lx + 22, my + 56, "cada esclavo", "muted")
rect(lx, my + 86, 14, 14, "acople", 3)
t(lx + 22, my + 98, "−N = acoples", "b")
t(lx + 22, my + 118, "entre franjas", "muted")
rect(lx, my + 148, 14, 14, "vacio", 3)
t(lx + 22, my + 160, "ceros", "b")
t(540, Y + 318, "Franjas ⇒ A es tridiagonal por bloques:", "b")
t(540, Y + 340, "cada bloque sólo ve al anterior y al siguiente.", "muted")
t(540, Y + 362, "(Eso habilita la aceleración de la sección 7.)", "muted")

# ---------------------------------------------------------------- 3. ronda
Y = 520
card(30, Y, 940, 230, 3, "Una ronda (iteración externa k)")
bx = [(60, "Coordinador", ["envía x^{k} a cada", "esclavo libre"]),
      (360, "Esclavo B (núcleo C++)", ["A_{BB}·x_{B} = b_{B} − A_{B,ext}·x_{ext}^{k}", "m barridos de Jacobi"]),
      (680, "Coordinador", ["junta x^{k+1}; para si", "‖b − A·x^{k+1}‖/‖b‖ ≤ tol"])]
for x, tit, lineas in bx:
    rect(x, Y + 62, 260, 96, "box", 10)
    t(x + 16, Y + 88, tit, "h3")
    for n, ln in enumerate(lineas):
        t(x + 16, Y + 114 + 22 * n, ln, "b")
arrow(322, Y + 110, 354, Y + 110)
arrow(642, Y + 110, 674, Y + 110)
out.append(f'<path d="M810,{Y + 160} v18 h-680 v-14" class="arrow" fill="none" marker-end="url(#punta)"/>')
t(470, Y + 196, "k ← k + 1", "muted", "middle")
t(60, Y + 214, "Error: e^{k+1} = (M^{−1}N)·e^{k}  ⇒  converge para todo x^{0} si y sólo si ρ(M^{−1}N) < 1;  rondas ≈ ln(tol) / ln ρ", "b")

# ---------------------------------------------------------------- 4. por qué
Y = 770
card(30, Y, 940, 290, 4, "Por qué ρ(M^{−1}N) < 1: la cadena de teoremas")
pasos = [
    ("La placa cumple", ["Z-matriz: a_{ij} ≤ 0", "débil: 4 ≥ 1+1+1+1", "irreducible: grilla conexa"]),
    ("⇒ M-matriz", ["A^{−1} ≥ 0 elemento a elemento", "simétrica ⇒ definida positiva", "(Varga)"]),
    ("⇒ Splitting regular", ["M = diag(A_{BB}): M^{−1} ≥ 0", "N = acoples cambiados", "de signo: N ≥ 0"]),
    ("⇒ Converge", ["ρ(M^{−1}N) < 1 con cualquier", "partición; bloques más", "grandes ⇒ ρ menor"]),
]
for n, (tit, lineas) in enumerate(pasos):
    x = 60 + n * 225
    rect(x, Y + 62, 200, 120, "box", 10)
    t(x + 14, Y + 88, tit, "h3")
    for m_, ln in enumerate(lineas):
        t(x + 14, Y + 114 + 21 * m_, ln, "small")
    if n < 3:
        arrow(x + 202, Y + 122, x + 222, Y + 122)
rect(60, Y + 200, 880, 66, "warn", 10)
out.append(f'<path d="M84,{Y + 244} l12,-22 l12,22 z" class="warnicon"/>')
t(96, Y + 241, "!", "warnmark", "middle")
t(122, Y + 226, "Atención: la dominancia débil sola no alcanza; hace falta la irreducibilidad.", "h3")
t(122, Y + 250, "[[1,−1,0],[−1,1,0],[0,0,2]] es débil con una fila estricta y es singular. es_diagonal_dominante() ahora lo verifica.", "small")

# ---------------------------------------------------------------- 5. bloques
Y = 1080
card(30, Y, 460, 330, 5, "Bloques más grandes, menos rondas")
t(60, Y + 70, "rondas estimadas ≈ ln(10^{−8}) / ln ρ", "muted")
datos5 = [("Jacobi puntual", 0.99707, 6269), ("16 bloques", 0.98586, 1294), ("8 bloques", 0.97171, 642),
          ("4 bloques (hoy)", 0.94780, 344), ("2 bloques", 0.91987, 221)]
x0, maxw = 190, 200
for n, (nom, rho, ron) in enumerate(datos5):
    y = Y + 96 + n * 44
    t(60, y + 12, nom, "lab")
    t(60, y + 30, f"ρ = {rho:.5f}".replace(".", ","), "tiny")
    w = max(3, maxw * ron / 6269)
    bar_h(x0, y, w, 22, f"{nom}: ρ = {rho}, ~{ron} rondas")
    t(x0 + w + 8, y + 16, f"{ron:,}".replace(",", " "), "val")
out.append(f'<line x1="{x0}" y1="{Y + 90}" x2="{x0}" y2="{Y + 312}" class="axis"/>')

# ---------------------------------------------------------------- 6. barridos internos
card(510, Y, 460, 330, 6, "¿Cuántos barridos internos (m)?")
t(540, Y + 70, "T_{m} = P^{m} + (I − P^{m})·M^{−1}N:  m = 1 es Jacobi puntual, m → ∞ por bloques", "tiny")
ms = ["1", "5", "10", "20", "100", "500"]
rondas = [4849, 1098, 646, 434, 288, 284]
barr = [9698, 6588, 7106, 9114, 29088, 123738]
for k_, (tit, vals, vmax) in enumerate((("rondas (red)", rondas, 4849), ("barridos (CPU)", barr, 123738))):
    ox, base = 548 + k_ * 214, Y + 270
    t(ox, Y + 104, tit, "lab")
    out.append(f'<line x1="{ox}" y1="{base}" x2="{ox + 190}" y2="{base}" class="axis"/>')
    for n, (m_, v) in enumerate(zip(ms, vals)):
        x = ox + 6 + n * 31
        h = max(3, 130 * v / vmax)
        bar_v(x, base, 20, h, f"m = {m_}: {v} {tit.split()[0]}")
        t(x + 10, base + 16, m_, "tiny", "middle")
        if (k_ == 0 and m_ in ("1", "500")) or (k_ == 1 and m_ in ("5", "500")):
            t(x + 10, base - h - 6, f"{v:,}".replace(",", " "), "tinyv", "middle")
t(540, Y + 312, "m = 5: mínimo cómputo · m ≥ 100: mínimas rondas · hoy m = 500", "small")

# ---------------------------------------------------------------- 7. acelerar
Y = 1430
card(30, Y, 940, 260, 7, "Cómo converger más rápido: cambiar el método externo")
datos7 = [("Jacobi por bloques (hoy)", 284), ("SOR rojo-negro, ω = 1,516", 60), ("Gradiente conjugado + Jacobi por bloques", 26)]
x0, maxw = 360, 210
for n, (nom, v) in enumerate(datos7):
    y = Y + 74 + n * 46
    t(60, y + 16, nom, "lab")
    w = maxw * v / 284
    bar_h(x0, y, w, 22, f"{nom}: {v} rondas")
    t(x0 + w + 8, y + 16, f"{v} rondas", "val")
out.append(f'<line x1="{x0}" y1="{Y + 68}" x2="{x0}" y2="{Y + 212}" class="axis"/>')
t(720, Y + 86, "A 2-cíclica (Young):", "h3")
t(720, Y + 108, "ρ_{GS} = ρ_{J}^{2},  ω_{opt} = 2/(1 + √(1 − ρ_{J}^{2}))", "small")
t(720, Y + 146, "Precondicionado (A y M", "h3")
t(720, Y + 166, "simétricas definidas positivas):", "h3")
t(720, Y + 188, "κ(M^{−1}A) ≈ 37 en vez de κ(A) ≈ 681", "small")
t(720, Y + 208, "⇒ iteraciones ∝ √κ", "small")
t(60, Y + 240, "Los esclavos hacen el mismo trabajo de hoy (resolver A_{BB}·z_{B} = r_{B}); el coordinador ya calcula A·x en cada ronda.", "muted")

# ---------------------------------------------------------------- 8. datos útiles
Y = 1710
tiles = [
    ("Residuo gratis", ["s_{i} = b_{i} − Σ_{j≠i} a_{ij}x_{j} da a la vez", "x_{i} ← s_{i}/a_{ii}  y  r_{i} = s_{i} − a_{ii}x_{i}"]),
    ("Residuo ≠ error", ["error rel. ≤ κ(A)·residuo rel., κ = 681", "medido: 1,2·10^{−7} vs 9,9·10^{−9}"]),
    ("Asíncrono también", ["T_{m} ≥ 0 ⇒ ρ(|T_{m}|) < 1: converge", "sin barrera (Chazan–Miranker)"]),
]
for n, (tit, lineas) in enumerate(tiles):
    x = 30 + n * 320
    rect(x, Y, 300, 104, "card", 14)
    t(x + 20, Y + 32, tit, "h3")
    for m_, ln in enumerate(lineas):
        t(x + 20, Y + 60 + 22 * m_, ln, "small")
t(500, Y + 140, "Números reproducibles con docs/experimentos_convergencia.py · detalle en docs/matematica.md", "muted", "middle")

H = Y + 164
css = """
svg{font-family:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}
.root{--bg:#f4f3ef;--card:#fcfcfb;--line:#e2e0d8;--ink:#0b0b0b;--ink2:#52514e;--ink3:#6f6e69;
 --s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--bar:#2a78d6;--acople:#c9c7bd;--vacio:#efeee9;
 --warnbg:#fff4dc;--cellop:.3;--warn:#fab219;--hot:#d03b3b;--box:#f4f3ef}
@media (prefers-color-scheme:dark){.root{--bg:#121211;--card:#1a1a19;--line:#33332f;--ink:#f0efec;--ink2:#c3c2b7;--ink3:#a3a29a;
 --s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--bar:#3987e5;--acople:#5a5953;--vacio:#262624;
 --warnbg:#3a2f12;--cellop:.5;--warn:#fab219;--hot:#e66767;--box:#232322}}
.bgr{fill:var(--bg)} .card{fill:var(--card);stroke:var(--line)} .box{fill:var(--box);stroke:var(--line)}
.num{fill:var(--s1)} .numt{fill:#fff;font-size:15px;font-weight:700}
.h1{fill:var(--ink);font-size:30px;font-weight:700} .sub{fill:var(--ink2);font-size:15px}
.h2{fill:var(--ink);font-size:19px;font-weight:650} .h3{fill:var(--ink);font-size:15px;font-weight:650}
.b{fill:var(--ink);font-size:14.5px} .muted{fill:var(--ink2);font-size:14px} .small{fill:var(--ink2);font-size:13px}
.lab{fill:var(--ink);font-size:13.5px} .tiny{fill:var(--ink3);font-size:12px} .tinyv{fill:var(--ink2);font-size:11.5px}
.val{fill:var(--ink2);font-size:13px;font-variant-numeric:tabular-nums}
.onfill{fill:#fff;font-size:13px;font-weight:650} .onfilld{fill:#0b0b0b;font-size:13px;font-weight:650}
.s1{fill:var(--s1)} .s2{fill:var(--s2)} .s3{fill:var(--s3)} .s4{fill:var(--s4)} .cell{opacity:var(--cellop)}
.sten{fill:none;stroke:var(--ink);stroke-width:2} .stenc{fill:var(--ink);opacity:.85}
.acople{fill:var(--acople)} .vacio{fill:var(--vacio)} .bar{fill:var(--bar)}
.hot{stroke:var(--hot);stroke-width:4;stroke-linecap:round}
.axis{stroke:var(--ink3);stroke-width:1} .arrow{stroke:var(--ink2);stroke-width:2}
.warn{fill:var(--warnbg);stroke:var(--warn)} .warnicon{fill:var(--warn)} .warnmark{fill:#0b0b0b;font-size:14px;font-weight:800}
#punta path{fill:var(--ink2)}
"""
svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="titulo desc">
<title id="titulo">Jacobi por bloques distribuido: por qué funciona</title>
<desc id="desc">Infografía: el problema de la placa repartido en franjas, la matriz A = M − N, una ronda coordinador-esclavos, la cadena M-matriz, splitting regular y ρ menor que 1, el efecto del tamaño de bloque y de los barridos internos, y cómo acelerar con SOR o gradiente conjugado.</desc>
<style>{css}</style>
<g class="root"><defs><marker id="punta" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z"/></marker></defs><rect class="bgr" width="{W}" height="{H}"/>
{chr(10).join(out)}
</g></svg>
"""
open(sys.argv[1], "w").write(svg)
print("ok", H)
