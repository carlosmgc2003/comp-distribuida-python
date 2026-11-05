#include "jacobi/jacobi.hpp"

#include <cmath>
#include <utility>

namespace jacobi {

std::string_view describir(Error e) noexcept {
    switch (e) {
        case Error::dimensiones_invalidas: return "dimensiones inválidas en la CSR o en los vectores";
        case Error::indice_fuera_de_rango: return "índice de columna fuera de rango";
        case Error::diagonal_nula: return "diagonal nula: Jacobi requiere a_ii != 0";
        case Error::divergencia: return "el método divergió (NaN/Inf); ¿la matriz es diagonal dominante?";
        case Error::opciones_invalidas: return "opciones inválidas (tolerancia o max_iteraciones negativas)";
    }
    return "error desconocido";
}

namespace {

/// Bloque preprocesado: separa diagonal, parte interna (fuera de la diagonal)
/// y el aporte constante de las columnas externas (que se suma a b una sola vez).
struct BloquePreparado {
    std::vector<double> diagonal;
    std::vector<double> b_efectivo;
    std::vector<Indice> indptr_int{0};
    std::vector<Indice> col_local;  // columna relativa al bloque
    std::vector<double> coef;
};

std::expected<BloquePreparado, Error> preparar(const VistaCSR& A, std::span<const double> b_local,
                                               std::span<const double> x_global) {
    const Indice n_filas = A.n_filas();
    const auto n = static_cast<Indice>(x_global.size());
    if (A.indptr.empty() || static_cast<Indice>(b_local.size()) != n_filas ||
        A.indices.size() != A.datos.size() || A.fila_inicial < 0 ||
        A.fila_inicial + n_filas > n) {
        return std::unexpected(Error::dimensiones_invalidas);
    }
    // indptr puede venir con un desplazamiento (vista de una matriz mayor).
    const Indice base = A.indptr.front();
    if (A.indptr.back() - base != static_cast<Indice>(A.indices.size())) {
        return std::unexpected(Error::dimensiones_invalidas);
    }

    BloquePreparado p;
    p.diagonal.assign(static_cast<std::size_t>(n_filas), 0.0);
    p.b_efectivo.assign(b_local.begin(), b_local.end());
    p.indptr_int.reserve(static_cast<std::size_t>(n_filas) + 1);
    p.col_local.reserve(A.indices.size());
    p.coef.reserve(A.indices.size());

    const Indice desde = A.fila_inicial;
    const Indice hasta = A.fila_inicial + n_filas;
    for (Indice i = 0; i < n_filas; ++i) {
        const auto ini = A.indptr[static_cast<std::size_t>(i)] - base;
        const auto fin = A.indptr[static_cast<std::size_t>(i) + 1] - base;
        if (ini > fin || ini < 0 || fin > static_cast<Indice>(A.indices.size())) {
            return std::unexpected(Error::dimensiones_invalidas);
        }
        const auto ii = static_cast<std::size_t>(i);
        for (auto k = static_cast<std::size_t>(ini); k < static_cast<std::size_t>(fin); ++k) {
            const Indice col = A.indices[k];
            const double a = A.datos[k];
            if (col < 0 || col >= n) return std::unexpected(Error::indice_fuera_de_rango);
            if (col == desde + i) {
                p.diagonal[ii] += a;  // suma por si hay entradas duplicadas
            } else if (col >= desde && col < hasta) {
                p.col_local.push_back(col - desde);
                p.coef.push_back(a);
            } else {
                // Columna externa: x_ext no cambia durante la resolución local.
                p.b_efectivo[ii] -= a * x_global[static_cast<std::size_t>(col)];
            }
        }
        if (p.diagonal[ii] == 0.0) return std::unexpected(Error::diagonal_nula);
        p.indptr_int.push_back(static_cast<Indice>(p.col_local.size()));
    }
    return p;
}

double norma2(std::span<const double> v) {
    double s = 0.0;
    for (double e : v) s += e * e;
    return std::sqrt(s);
}

}  // namespace

std::expected<Resultado, Error> resolver_bloque(const VistaCSR& A, std::span<const double> b_local,
                                                std::span<const double> x_global,
                                                const Opciones& opciones) {
    if (opciones.tolerancia < 0.0 || opciones.max_iteraciones < 0) {
        return std::unexpected(Error::opciones_invalidas);
    }
    auto preparado = preparar(A, b_local, x_global);
    if (!preparado) return std::unexpected(preparado.error());
    const BloquePreparado& p = *preparado;

    const auto n_filas = static_cast<std::size_t>(A.n_filas());
    const auto inicio = x_global.begin() + A.fila_inicial;
    std::vector<double> x(inicio, inicio + static_cast<std::ptrdiff_t>(n_filas));
    std::vector<double> x_nuevo(n_filas);

    double norma_b = norma2(p.b_efectivo);
    if (norma_b == 0.0) norma_b = 1.0;  // b_eff = 0 -> usamos residuo absoluto

    for (int it = 0;; ++it) {
        // Un barrido calcula a la vez x^(k+1) y el residuo de x^(k):
        //   s_i = b_i - Σ_{j≠i} a_ij x_j     =>  r_i = s_i - a_ii x_i
        // así el residuo informado es exacto y no cuesta un producto extra.
        double r2 = 0.0;
        for (std::size_t i = 0; i < n_filas; ++i) {
            double s = p.b_efectivo[i];
            const auto fin = static_cast<std::size_t>(p.indptr_int[i + 1]);
            for (auto k = static_cast<std::size_t>(p.indptr_int[i]); k < fin; ++k) {
                s -= p.coef[k] * x[static_cast<std::size_t>(p.col_local[k])];
            }
            const double r = s - p.diagonal[i] * x[i];
            r2 += r * r;
            x_nuevo[i] = s / p.diagonal[i];
        }
        const double residuo = std::sqrt(r2) / norma_b;
        if (!std::isfinite(residuo)) return std::unexpected(Error::divergencia);
        const bool convergio = residuo <= opciones.tolerancia;
        if (convergio || it == opciones.max_iteraciones) {
            return Resultado{.x = std::move(x), .iteraciones = it, .residuo = residuo,
                             .convergio = convergio};
        }
        std::swap(x, x_nuevo);
    }
}

VistaCSR MatrizCSR::vista(Indice desde, Indice hasta) const {
    const auto d = static_cast<std::size_t>(desde);
    const auto h = static_cast<std::size_t>(hasta);
    const auto ini = static_cast<std::size_t>(indptr[d]);
    const auto fin = static_cast<std::size_t>(indptr[h]);
    return VistaCSR{
        .indptr = std::span(indptr).subspan(d, h - d + 1),
        .indices = std::span(indices).subspan(ini, fin - ini),
        .datos = std::span(datos).subspan(ini, fin - ini),
        .fila_inicial = desde,
    };
}

MatrizCSR MatrizCSR::desde_densa(std::span<const double> valores, Indice n_filas,
                                 Indice n_columnas) {
    MatrizCSR m;
    m.n_columnas = n_columnas;
    m.indptr.reserve(static_cast<std::size_t>(n_filas) + 1);
    for (Indice i = 0; i < n_filas; ++i) {
        for (Indice j = 0; j < n_columnas; ++j) {
            const double v = valores[static_cast<std::size_t>(i * n_columnas + j)];
            if (v != 0.0) {
                m.indices.push_back(j);
                m.datos.push_back(v);
            }
        }
        m.indptr.push_back(static_cast<Indice>(m.indices.size()));
    }
    return m;
}

double residuo_relativo(const MatrizCSR& A, std::span<const double> b, std::span<const double> x) {
    double r2 = 0.0;
    for (Indice i = 0; i < A.n_filas(); ++i) {
        const auto ii = static_cast<std::size_t>(i);
        double r = b[ii];
        for (auto k = static_cast<std::size_t>(A.indptr[ii]);
             k < static_cast<std::size_t>(A.indptr[ii + 1]); ++k) {
            r -= A.datos[k] * x[static_cast<std::size_t>(A.indices[k])];
        }
        r2 += r * r;
    }
    const double nb = norma2(b);
    return std::sqrt(r2) / (nb == 0.0 ? 1.0 : nb);
}

}  // namespace jacobi
