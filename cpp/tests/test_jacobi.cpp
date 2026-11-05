#include "jacobi/jacobi.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cmath>
#include <vector>

using Catch::Matchers::WithinAbs;
using jacobi::Error;
using jacobi::MatrizCSR;

namespace {

/// Laplaciano 2D de 5 puntos sobre una placa k×k (diagonal 4, vecinos -1),
/// con el borde superior a 100 grados: el problema típico del TP.
struct Placa {
    MatrizCSR A;
    std::vector<double> b;
};

Placa placa(int k) {
    const int n = k * k;
    std::vector<double> densa(static_cast<std::size_t>(n * n), 0.0);
    std::vector<double> b(static_cast<std::size_t>(n), 0.0);
    for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
            const int p = i * k + j;
            densa[static_cast<std::size_t>(p * n + p)] = 4.0;
            const int di[] = {1, -1, 0, 0};
            const int dj[] = {0, 0, 1, -1};
            for (int d = 0; d < 4; ++d) {
                const int ii = i + di[d];
                const int jj = j + dj[d];
                if (ii >= 0 && ii < k && jj >= 0 && jj < k) {
                    densa[static_cast<std::size_t>(p * n + ii * k + jj)] = -1.0;
                } else if (ii < 0) {
                    b[static_cast<std::size_t>(p)] += 100.0;
                }
            }
        }
    }
    return {MatrizCSR::desde_densa(densa, n, n), b};
}

}  // namespace

TEST_CASE("Sistema diagonal: converge en una iteración") {
    const std::vector<double> densa{2, 0, 0, 0, 4, 0, 0, 0, 8};
    const auto A = MatrizCSR::desde_densa(densa, 3, 3);
    const std::vector<double> b{2, 4, 8};
    const std::vector<double> x0(3, 0.0);
    const auto r = jacobi::resolver_bloque(A.vista(), b, x0);
    REQUIRE(r.has_value());
    CHECK(r->convergio);
    CHECK(r->iteraciones == 1);
    for (double v : r->x) CHECK_THAT(v, WithinAbs(1.0, 1e-15));
}

TEST_CASE("Divide por la diagonal (bug del original: asumía a_ii = 1)") {
    // 4x - y = 3 ; -x + 4y = 3  =>  x = y = 1
    const std::vector<double> densa{4, -1, -1, 4};
    const auto A = MatrizCSR::desde_densa(densa, 2, 2);
    const std::vector<double> b{3, 3};
    const std::vector<double> x0(2, 0.0);
    const auto r = jacobi::resolver_bloque(A.vista(), b, x0, {.tolerancia = 1e-14});
    REQUIRE(r.has_value());
    CHECK(r->convergio);
    CHECK_THAT(r->x[0], WithinAbs(1.0, 1e-13));
    CHECK_THAT(r->x[1], WithinAbs(1.0, 1e-13));
}

TEST_CASE("Placa completa en un solo bloque") {
    const auto [A, b] = placa(8);
    const std::vector<double> x0(b.size(), 0.0);
    const auto r = jacobi::resolver_bloque(A.vista(), b, x0, {.tolerancia = 1e-12, .max_iteraciones = 100'000});
    REQUIRE(r.has_value());
    CHECK(r->convergio);
    // El residuo informado es el EXACTO de la x devuelta.
    CHECK_THAT(jacobi::residuo_relativo(A, b, r->x), WithinAbs(r->residuo, 1e-15));
}

TEST_CASE("Jacobi por bloques converge a la misma solución (bug del original: semillas mal indexadas)") {
    const auto [A, b] = placa(8);
    const auto n = static_cast<jacobi::Indice>(b.size());
    // 3 bloques de tamaños distintos: el último NO es múltiplo del tamaño de lote.
    const std::vector<jacobi::Indice> cortes{0, 25, 50, n};
    std::vector<double> x(b.size(), 0.0);
    double residuo = 1.0;
    int externas = 0;
    for (; externas < 2000 && residuo > 1e-10; ++externas) {
        std::vector<double> x_nuevo(x.size());
        for (std::size_t k = 0; k + 1 < cortes.size(); ++k) {
            const auto vista = A.vista(cortes[k], cortes[k + 1]);
            const std::span b_local = std::span(b).subspan(static_cast<std::size_t>(cortes[k]),
                                                           static_cast<std::size_t>(cortes[k + 1] - cortes[k]));
            const auto r = jacobi::resolver_bloque(vista, b_local, x, {.tolerancia = 1e-12});
            REQUIRE(r.has_value());
            std::ranges::copy(r->x, x_nuevo.begin() + cortes[k]);
        }
        x = std::move(x_nuevo);
        residuo = jacobi::residuo_relativo(A, b, x);
    }
    INFO("iteraciones externas: " << externas);
    CHECK(residuo <= 1e-10);
}

TEST_CASE("Errores se informan con std::expected, sin UB") {
    const std::vector<double> x0(2, 0.0);
    const std::vector<double> b{1, 1};

    SECTION("diagonal nula") {
        const std::vector<double> densa{0, 1, 1, 0};
        const auto A = MatrizCSR::desde_densa(densa, 2, 2);
        const auto r = jacobi::resolver_bloque(A.vista(), b, x0);
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error() == Error::diagonal_nula);
    }
    SECTION("b con tamaño incorrecto") {
        const std::vector<double> densa{1, 0, 0, 1};
        const auto A = MatrizCSR::desde_densa(densa, 2, 2);
        const std::vector<double> b_corto{1};
        const auto r = jacobi::resolver_bloque(A.vista(), b_corto, x0);
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error() == Error::dimensiones_invalidas);
    }
    SECTION("columna fuera de rango") {
        const std::vector<jacobi::Indice> indptr{0, 2};
        const std::vector<jacobi::Indice> indices{0, 7};
        const std::vector<double> datos{1, 1};
        const jacobi::VistaCSR A{.indptr = indptr, .indices = indices, .datos = datos};
        const std::vector<double> b1{1};
        const auto r = jacobi::resolver_bloque(A, b1, x0);
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error() == Error::indice_fuera_de_rango);
    }
    SECTION("matriz no diagonal dominante: diverge y se detecta (el original iteraba para siempre)") {
        const std::vector<double> densa{1, 3, 3, 1};
        const auto A = MatrizCSR::desde_densa(densa, 2, 2);
        const auto r = jacobi::resolver_bloque(A.vista(), b, x0, {.max_iteraciones = 100'000});
        REQUIRE_FALSE(r.has_value());
        CHECK(r.error() == Error::divergencia);
    }
}

TEST_CASE("max_iteraciones acota el trabajo y se informa que no convergió") {
    const auto [A, b] = placa(6);
    const std::vector<double> x0(b.size(), 0.0);
    const auto r = jacobi::resolver_bloque(A.vista(), b, x0, {.tolerancia = 1e-15, .max_iteraciones = 5});
    REQUIRE(r.has_value());
    CHECK_FALSE(r->convergio);
    CHECK(r->iteraciones == 5);
    CHECK_THAT(jacobi::residuo_relativo(A, b, r->x), WithinAbs(r->residuo, 1e-14));
}

TEST_CASE("Una fila con sólo la diagonal también se resuelve (el original la perdía)") {
    const std::vector<double> densa{5, 0, 1, 4};
    const auto A = MatrizCSR::desde_densa(densa, 2, 2);
    const std::vector<double> b{10, 6};
    const std::vector<double> x0(2, 0.0);
    const auto r = jacobi::resolver_bloque(A.vista(), b, x0, {.tolerancia = 1e-14});
    REQUIRE(r.has_value());
    REQUIRE(r->x.size() == 2);
    CHECK_THAT(r->x[0], WithinAbs(2.0, 1e-13));
    CHECK_THAT(r->x[1], WithinAbs(1.0, 1e-13));
}
