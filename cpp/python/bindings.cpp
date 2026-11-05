// Módulo de Python `jacobi_nativo` (nanobind): expone resolver_bloque sin copias.
//
// Reemplaza el esquema original "Python escribe un .db SQLite -> subprocess ->
// C++ lee el .db -> escribe otro .db -> Python lo lee". Ahora el esclavo llama a
// la función directamente con arrays de NumPy, en el mismo proceso.
#include "jacobi/jacobi.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;
using namespace nb::literals;

namespace {

template <typename T>
using Vec1D = nb::ndarray<const T, nb::ndim<1>, nb::c_contig, nb::device::cpu>;

template <typename T>
std::span<const T> como_span(const Vec1D<T>& a) {
    return {a.data(), a.shape(0)};
}

nb::tuple resolver_bloque_py(const Vec1D<jacobi::Indice>& indptr,
                             const Vec1D<jacobi::Indice>& indices, const Vec1D<double>& datos,
                             jacobi::Indice fila_inicial, const Vec1D<double>& b_local,
                             const Vec1D<double>& x_global, double tolerancia,
                             int max_iteraciones) {
    const jacobi::VistaCSR A{
        .indptr = como_span(indptr),
        .indices = como_span(indices),
        .datos = como_span(datos),
        .fila_inicial = fila_inicial,
    };
    const jacobi::Opciones opciones{.tolerancia = tolerancia, .max_iteraciones = max_iteraciones};

    std::expected<jacobi::Resultado, jacobi::Error> resultado;
    {
        // El cálculo no toca objetos de Python: liberamos el GIL para que otros
        // hilos (p.ej. el que atiende el socket) sigan funcionando.
        nb::gil_scoped_release sin_gil;
        resultado = jacobi::resolver_bloque(A, como_span(b_local), como_span(x_global), opciones);
    }
    if (!resultado) throw nb::value_error(std::string(jacobi::describir(resultado.error())).c_str());

    // Transferimos la propiedad del vector a NumPy (sin copiar los datos).
    auto* x = new std::vector<double>(std::move(resultado->x));
    nb::capsule propietario(x, [](void* p) noexcept { delete static_cast<std::vector<double>*>(p); });
    nb::ndarray<nb::numpy, double, nb::ndim<1>> x_np(x->data(), {x->size()}, propietario);

    return nb::make_tuple(x_np, resultado->iteraciones, resultado->residuo,
                          resultado->convergio);
}

}  // namespace

NB_MODULE(jacobi_nativo, m) {
    m.doc() = "Núcleo en C++ del método de Jacobi por bloques (formato CSR).";
    m.def("resolver_bloque", &resolver_bloque_py, "indptr"_a, "indices"_a, "datos"_a,
          "fila_inicial"_a, "b_local"_a, "x_global"_a, "tolerancia"_a = 1e-10,
          "max_iteraciones"_a = 10'000,
          "Resuelve con Jacobi las filas [fila_inicial, fila_inicial + len(b_local)).\n\n"
          "indptr/indices se esperan int64 y el resto float64 (si no, se convierten con una copia).\n"
          "Devuelve (x, iteraciones, residuo_relativo, convergio).\n"
          "Lanza ValueError si los datos son inválidos o el método diverge.");
}
