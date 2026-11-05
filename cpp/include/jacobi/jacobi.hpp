// Núcleo numérico: método de Jacobi aplicado a un bloque de filas de un sistema
// disperso A·x = b almacenado en formato CSR (Compressed Sparse Row).
//
// Idea del método por bloques (lo que hace cada esclavo):
//
//   Las filas del bloque B se separan en columnas internas (las del propio bloque)
//   y externas (las de otros bloques):
//
//       A_BB · x_B = b_B - A_B,ext · x_ext          (x_ext queda fijo)
//
//   y ese sistema local se resuelve con Jacobi:
//
//       x_i^(k+1) = ( b_eff_i - Σ_{j≠i, j∈B} a_ij · x_j^(k) ) / a_ii
//
// El coordinador junta los x_B de todos los bloques y repite (iteración externa).
#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

namespace jacobi {

using Indice = std::int64_t;

/// Vista (sin copia, sin propiedad) de un bloque de filas consecutivas en CSR.
///
/// La fila local `i` (0 <= i < n_filas) corresponde a la fila global
/// `fila_inicial + i`; sus elementos son `indices[indptr[i] .. indptr[i+1])`
/// (columnas GLOBALES) y `datos[...]` (coeficientes).
struct VistaCSR {
    std::span<const Indice> indptr;   // tamaño n_filas + 1, indptr[0] == 0
    std::span<const Indice> indices;  // columnas globales, tamaño nnz
    std::span<const double> datos;    // coeficientes, tamaño nnz
    Indice fila_inicial = 0;

    [[nodiscard]] constexpr Indice n_filas() const noexcept {
        return indptr.empty() ? 0 : static_cast<Indice>(indptr.size()) - 1;
    }
};

struct Opciones {
    /// Se detiene cuando ||r||_2 / ||b_eff||_2 <= tolerancia (residuo relativo).
    double tolerancia = 1e-10;
    /// Cota dura: nunca más de esta cantidad de barridos (evita bucles infinitos).
    int max_iteraciones = 10'000;
};

struct Resultado {
    std::vector<double> x;   // solución local (n_filas valores)
    int iteraciones = 0;     // barridos de Jacobi aplicados
    double residuo = 0.0;    // residuo relativo EXACTO de `x`
    bool convergio = false;  // residuo <= tolerancia
};

enum class Error {
    dimensiones_invalidas,  // tamaños de spans incoherentes
    indice_fuera_de_rango,  // columna < 0 o >= x_global.size()
    diagonal_nula,          // a_ii == 0: Jacobi no está definido
    divergencia,            // aparecieron NaN/Inf (p.ej. matriz no diagonal dominante)
    opciones_invalidas,     // tolerancia < 0 o max_iteraciones < 0
};

[[nodiscard]] std::string_view describir(Error e) noexcept;

/// Resuelve el bloque `A` (filas fila_inicial .. fila_inicial+n_filas-1) con Jacobi.
///
/// - `b_local`: término independiente de las filas del bloque (n_filas valores).
/// - `x_global`: estimación actual de TODO el vector solución (n valores). Se usa
///   para las columnas externas y como punto de partida de las internas.
///
/// Nunca lanza excepciones por datos inválidos: devuelve `std::unexpected(Error)`.
[[nodiscard]] std::expected<Resultado, Error> resolver_bloque(const VistaCSR& A,
                                                              std::span<const double> b_local,
                                                              std::span<const double> x_global,
                                                              const Opciones& opciones = {});

/// Matriz CSR con almacenamiento propio (útil para la CLI y los tests).
struct MatrizCSR {
    std::vector<Indice> indptr{0};
    std::vector<Indice> indices;
    std::vector<double> datos;
    Indice n_columnas = 0;

    [[nodiscard]] Indice n_filas() const noexcept { return static_cast<Indice>(indptr.size()) - 1; }

    /// Vista de las filas [desde, hasta). Los índices de indptr quedan
    /// relativos al bloque (se re-basan en `resolver_bloque`).
    [[nodiscard]] VistaCSR vista(Indice desde, Indice hasta) const;
    [[nodiscard]] VistaCSR vista() const { return vista(0, n_filas()); }

    /// Construye la CSR a partir de una matriz densa por filas (descarta ceros).
    [[nodiscard]] static MatrizCSR desde_densa(std::span<const double> valores, Indice n_filas,
                                               Indice n_columnas);
};

/// ||b - A·x||_2 / ||b||_2 para la matriz completa (para verificar resultados).
[[nodiscard]] double residuo_relativo(const MatrizCSR& A, std::span<const double> b,
                                      std::span<const double> x);

}  // namespace jacobi
