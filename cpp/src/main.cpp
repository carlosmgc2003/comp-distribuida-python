// CLI autónoma: resuelve A·x = b completo (un único bloque) leyendo el formato
// de texto original del TP (matriz densa y vector, separados por espacios).
//
//   jacobi_cli matriz.txt vector.txt [--tol 1e-10] [--max-iter 100000] [-o x.txt]
//
// Sirve para probar el núcleo sin la parte distribuida.
#include "jacobi/jacobi.hpp"

#include <charconv>
#include <cstdlib>
#include <format>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

struct Tabla {
    std::vector<double> valores;
    std::int64_t filas = 0;
    std::int64_t columnas = 0;
};

std::optional<double> a_double(std::string_view s) {
    double v{};
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || ptr != s.data() + s.size()) return std::nullopt;
    return v;
}

/// Lee una tabla de números separados por cualquier cantidad de espacios/tabs.
/// Ignora líneas vacías y exige que todas las filas tengan el mismo largo.
std::optional<Tabla> leer_tabla(const std::string& ruta) {
    std::ifstream archivo(ruta);
    if (!archivo) {
        std::cerr << std::format("No se pudo abrir '{}'\n", ruta);
        return std::nullopt;
    }
    Tabla t;
    std::string linea;
    for (int nro = 1; std::getline(archivo, linea); ++nro) {
        std::istringstream campos(linea);
        std::string campo;
        std::int64_t cols = 0;
        while (campos >> campo) {
            const auto v = a_double(campo);
            if (!v) {
                std::cerr << std::format("{}:{}: '{}' no es un número\n", ruta, nro, campo);
                return std::nullopt;
            }
            t.valores.push_back(*v);
            ++cols;
        }
        if (cols == 0) continue;
        if (t.filas > 0 && cols != t.columnas) {
            std::cerr << std::format("{}:{}: se esperaban {} columnas y hay {}\n", ruta, nro,
                                     t.columnas, cols);
            return std::nullopt;
        }
        t.columnas = cols;
        ++t.filas;
    }
    return t;
}

int uso(std::string_view programa) {
    std::cerr << std::format(
        "Uso: {} matriz.txt vector.txt [--tol T] [--max-iter N] [-o salida.txt]\n", programa);
    return EXIT_FAILURE;
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::vector<std::string_view> args(argv, argv + argc);
    std::vector<std::string> posicionales;
    jacobi::Opciones opciones{.tolerancia = 1e-10, .max_iteraciones = 100'000};
    std::optional<std::string> salida;

    for (std::size_t i = 1; i < args.size(); ++i) {
        const auto siguiente = [&]() -> std::optional<std::string_view> {
            if (i + 1 >= args.size()) return std::nullopt;
            return args[++i];
        };
        if (args[i] == "--tol") {
            const auto v = siguiente().and_then(a_double);
            if (!v) return uso(args[0]);
            opciones.tolerancia = *v;
        } else if (args[i] == "--max-iter") {
            const auto v = siguiente().and_then(a_double);
            if (!v) return uso(args[0]);
            opciones.max_iteraciones = static_cast<int>(*v);
        } else if (args[i] == "-o") {
            const auto v = siguiente();
            if (!v) return uso(args[0]);
            salida = std::string(*v);
        } else if (args[i] == "-h" || args[i] == "--help") {
            uso(args[0]);
            return EXIT_SUCCESS;
        } else {
            posicionales.emplace_back(args[i]);
        }
    }
    if (posicionales.size() != 2) return uso(args[0]);

    const auto matriz = leer_tabla(posicionales[0]);
    const auto vector = leer_tabla(posicionales[1]);
    if (!matriz || !vector) return EXIT_FAILURE;
    if (matriz->filas != matriz->columnas || matriz->filas != std::ssize(vector->valores)) {
        std::cerr << std::format("Dimensiones incompatibles: A es {}x{} y b tiene {} valores\n",
                                 matriz->filas, matriz->columnas, vector->valores.size());
        return EXIT_FAILURE;
    }

    const auto A = jacobi::MatrizCSR::desde_densa(matriz->valores, matriz->filas, matriz->columnas);
    const std::vector<double> x0(static_cast<std::size_t>(A.n_filas()), 0.0);
    const auto resultado = jacobi::resolver_bloque(A.vista(), vector->valores, x0, opciones);
    if (!resultado) {
        std::cerr << std::format("Error: {}\n", jacobi::describir(resultado.error()));
        return EXIT_FAILURE;
    }

    std::cerr << std::format("{} en {} iteraciones, residuo relativo = {:.3e}\n",
                             resultado->convergio ? "Convergió" : "NO convergió",
                             resultado->iteraciones,
                             jacobi::residuo_relativo(A, vector->valores, resultado->x));

    std::ofstream archivo;
    if (salida) {
        archivo.open(*salida);
        if (!archivo) {
            std::cerr << std::format("No se pudo crear '{}'\n", *salida);
            return EXIT_FAILURE;
        }
    }
    std::ostream& out = salida ? archivo : std::cout;
    for (double v : resultado->x) out << std::format("{:.17g}\n", v);  // precisión completa
    return resultado->convergio ? EXIT_SUCCESS : 2;
}
